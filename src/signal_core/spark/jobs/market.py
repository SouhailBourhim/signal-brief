"""`silver.market_observations`, one row per `(ticker, trade_date)`. SPEC §7.4; ADR-0010.

Bronze `market` partitions -> daily OHLCV bars. Structurally the third of `normalize.py`'s
sibling passes, kept in its own module because it is the only silver table fed by a source
that restates its own history.

## The MERGE updates on match, and that is the difference

`silver.articles` and `silver.hn_comments` insert only: nothing in a published article
legitimately changes after first sight, so an UPDATE clause there would be a way to silently
rewrite history. Prices are the opposite. A split or a dividend restates every prior bar, and
the restated number is the *correct* one — refusing to overwrite would leave the table holding
pre-split prices that no longer describe anything. So this one is `WHEN MATCHED THEN UPDATE
SET *`, keyed on `(ticker, trade_date)`, matching `cost_snapshot.record`'s shape.

That is also what makes replay safe here (SPEC §6.3): every fetch re-states ~63 days, so
re-running a window converges on the same table rather than accumulating duplicates.

## Not bitemporal, and not gold

SPEC §9 lists `macro_observations` under gold with `valid_time`/`known_time` axes. That is
§8's ALFRED work in 4B, and it is a different claim: ALFRED serves *every vintage*, so the
question "what was knowable on date X" is answerable. Yahoo serves only the current view of
history, so there is no vintage to record and a `known_time` column here would be a fiction —
it would say "as of the last time we fetched", which is not a vintage. When 4B builds the
real bitemporal store, this table is a candidate to fold into it; until then it stays an
honest single-axis table (SPEC §17).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from signal_core.parse import get_parser
from signal_core.timeutil import ensure_utc

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

MARKET_TABLE = "silver.market_observations"
BRONZE_TABLE = "bronze.raw_documents"

MARKET_DDL = """
    ticker string NOT NULL,
    trade_date date NOT NULL,
    open double,
    high double,
    low double,
    close double NOT NULL,
    volume double,
    observed_at timestamp NOT NULL,
    ingest_id string NOT NULL
"""

# Same columns, no constraints: Spark's DataFrame schema parser rejects `NOT NULL`.
MARKET_ROW_SCHEMA = """
    ticker string, trade_date date, open double, high double, low double,
    close double, volume double, observed_at timestamp, ingest_id string
"""

MARKET_COLUMNS = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "observed_at",
    "ingest_id",
]


@dataclass(frozen=True)
class MarketResult:
    market_rows: int
    observations_extracted: int
    observations_committed: int
    table_rows: int


def ensure_table(spark: SparkSession, table: str = MARKET_TABLE) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({MARKET_DDL})
        USING iceberg
        PARTITIONED BY (months(trade_date))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.isolation-level' = 'serializable'
        )
        """
    )


def _extract_row(row: dict) -> list[dict]:
    result = get_parser("market")(row["payload"])
    return [
        {
            "ticker": o.ticker,
            "trade_date": o.trade_date,
            "open": o.open,
            "high": o.high,
            "low": o.low,
            "close": o.close,
            "volume": o.volume,
            # When this bar was *observed*, distinct from the day it describes. Two fetches
            # of the same trade_date differ only here, and the MERGE keeps the newer one.
            "observed_at": row["fetched_at"],
            "ingest_id": row.get("ingest_id", ""),
        }
        for o in result.market_observations
    ]


def _extract_partitions(iterator):
    import pandas as pd

    for pdf in iterator:
        rows = [row for record in pdf.to_dict("records") for row in _extract_row(record)]
        yield pd.DataFrame(rows, columns=MARKET_COLUMNS)


def _bronze_window(
    spark: SparkSession, since: datetime, until: datetime, *, table: str
) -> DataFrame:
    from pyspark.sql import functions as F

    since, until = ensure_utc(since), ensure_utc(until)
    return spark.table(table).where(
        (F.col("ingest_date") >= F.lit(since.date().isoformat()))
        & (F.col("ingest_date") <= F.lit(until.date().isoformat()))
        & (F.col("fetched_at") >= F.lit(since))
        & (F.col("fetched_at") < F.lit(until))
        & (F.col("source_id") == "market")
    )


def market_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    bronze_table: str = BRONZE_TABLE,
    market_table: str = MARKET_TABLE,
) -> MarketResult:
    """Bronze window, `market` partitions only -> `silver.market_observations`, MERGEd."""
    from pyspark.sql import functions as F

    ensure_table(spark, market_table)

    windowed = _bronze_window(spark, since, until, table=bronze_table)
    market_rows = windowed.where(F.col("outcome") == "ok").count()

    extracted = windowed.where(F.col("outcome") == "ok").mapInPandas(
        _extract_partitions, schema=MARKET_ROW_SCHEMA
    )
    # Within one window the same ticker/date can arrive twice — two fetches whose ~63-day
    # ranges overlap, which is every pair of consecutive fetches. Keep the latest
    # observation, since that is the one carrying any restatement.
    from pyspark.sql import Window

    newest = Window.partitionBy("ticker", "trade_date").orderBy(F.col("observed_at").desc())
    extracted = (
        extracted.withColumn("_rank", F.row_number().over(newest))
        .where(F.col("_rank") == 1)
        .drop("_rank")
        .select(*MARKET_COLUMNS)
        .localCheckpoint(eager=True)
    )
    observations_extracted = extracted.count()

    extracted.createOrReplaceTempView("_new_market_observations")
    before = spark.table(market_table).count()
    spark.sql(
        f"""
        MERGE INTO {market_table} AS target
        USING _new_market_observations AS source
        ON target.ticker = source.ticker AND target.trade_date = source.trade_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table(market_table).count()

    return MarketResult(
        market_rows=market_rows,
        observations_extracted=observations_extracted,
        observations_committed=after - before,
        table_rows=after,
    )
