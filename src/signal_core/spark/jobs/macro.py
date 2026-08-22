"""`gold.macro_observations` — the bitemporal store. SPEC §8, §9; docs/runbooks/phase-4b.md.

The differentiator the README leads with. Macro data is revised for months after first
publication; a normal pipeline overwrites and quietly destroys the record, and this one keeps
both time axes so "what was knowable on 2026-03-14" is a query rather than an archaeology
project.

    series_id, period, value, vintage_date, is_latest, revision_delta

- `period` is **valid time** — the month or day the number describes.
- `vintage_date` is **known time** — the day that value became the published figure.

## Insert-only, by nature rather than by policy

**A published vintage never changes.** ALFRED cannot retract what it said on 2026-07-02; a
correction is a *new* vintage on a later date. So the natural key `(series_id, period,
vintage_date)` is immutable once written, the MERGE has no UPDATE branch worth writing, and
replay is trivially correct — re-reading the same window inserts nothing. That is a stronger
guarantee than `market.py` gets, where a split adjustment genuinely restates an old bar.

## `is_latest` and `revision_delta` are derived, never fetched

Both are relationships between vintages, and a parser only ever sees one at a time. They are
recomputed with a window over each `(series_id, period)` **after** the merge, across the
whole table rather than the incoming batch — because a new vintage for June demotes June's
previous `is_latest`, and a row the batch never touched has to change.

`revision_delta` is `value - (previous vintage's value)`. The first vintage of a period has
**no previous value and therefore a null delta, not zero**. "Not yet revised" and "revised by
zero" are different facts about the world, and §8's whole argument is that collapsing facts
about revisions is how pipelines lose them. A revision from or to a missing value is likewise
null rather than treated as a move from nothing.

## Spark, not Athena

Unlike the enrichment tables, this is a bulk load with window functions over full vintage
history, which is Spark's shape — and it is off the 07:00 path entirely, so the argument
`brief/items.py` makes for staying JVM-free does not apply.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from signal_core.parse import get_parser
from signal_core.parse.macro import series_id_from_url
from signal_core.spark.tables import ensure_columns
from signal_core.timeutil import ensure_utc

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

MACRO_TABLE = "gold.macro_observations"
BRONZE_TABLE = "bronze.raw_documents"

MACRO_DDL = """
    series_id string NOT NULL,
    period date NOT NULL,
    value double,
    vintage_date date NOT NULL,
    superseded_at date,
    is_latest boolean NOT NULL,
    revision_delta double,
    observed_at timestamp NOT NULL,
    ingest_id string NOT NULL
"""

# Same columns, no constraints: Spark's DataFrame schema parser rejects `NOT NULL`.
MACRO_ROW_SCHEMA = """
    series_id string, period date, value double, vintage_date date, superseded_at date,
    observed_at timestamp, ingest_id string
"""

# What `_extract_partitions` emits — the derived columns are added after the merge, so they
# are deliberately absent here.
EXTRACT_COLUMNS = [
    "series_id",
    "period",
    "value",
    "vintage_date",
    "superseded_at",
    "observed_at",
    "ingest_id",
]


@dataclass(frozen=True)
class MacroResult:
    """What one load did. Every field is something a person would ask about afterwards."""

    bronze_rows: int
    observations_extracted: int
    observations_committed: int
    series_seen: int
    revisions_found: int
    table_rows: int


def ensure_table(spark: SparkSession, table: str = MACRO_TABLE) -> list[str]:
    """Create the table if absent, add any column the DDL has gained since. Returns what was
    added — 3.D's defect, which recurred in 4A on `ops.maintenance_runs`, is that
    `CREATE TABLE IF NOT EXISTS` is a no-op against a live table."""
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({MACRO_DDL})
        USING iceberg
        PARTITIONED BY (series_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.isolation-level' = 'serializable'
        )
        """
    )
    return ensure_columns(spark, table, MACRO_DDL)


def _extract_row(row: dict) -> list[dict]:
    """One bronze row — one series' vintage history — into observation dicts.

    The series id comes from the recorded request URL, not the payload: FRED does not echo it
    in the body. See `parse/macro.py::series_id_from_url`.
    """
    result = get_parser("macro")(row["payload"])
    series_id = series_id_from_url(row.get("source_url"))
    if not series_id:
        # Without it every observation would land under an empty series id and silently
        # merge six unrelated series into one. Dropping the row is the safe failure: the
        # bytes are still in bronze and a fixed extractor replays them.
        return []
    return [
        {
            "series_id": series_id,
            "period": o.period,
            "value": o.value,
            "vintage_date": o.vintage_date,
            "superseded_at": o.superseded_at,
            "observed_at": row["fetched_at"],
            "ingest_id": row.get("ingest_id", ""),
        }
        for o in result.macro_observations
    ]


def _extract_partitions(iterator):
    import pandas as pd

    for pdf in iterator:
        rows = [row for record in pdf.to_dict("records") for row in _extract_row(record)]
        yield pd.DataFrame(rows, columns=EXTRACT_COLUMNS)


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
        & (F.col("source_id") == "macro")
    )


def recompute_derived(spark: SparkSession, table: str = MACRO_TABLE) -> int:
    """Recompute `is_latest` and `revision_delta` across the whole table. Returns revisions.

    **Whole table, not the incoming batch.** A new vintage for June demotes June's previous
    `is_latest` and gives the new row a delta against it — both are rows the batch may never
    have touched. Scoping this to the batch would leave two rows claiming to be current,
    which is the single most damaging thing a bitemporal store can get wrong, because every
    "what is the number now" query silently doubles.

    Cheap enough to do unconditionally: this table is six series of a few thousand rows each,
    and correctness here is worth far more than the scan.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    by_vintage = Window.partitionBy("series_id", "period").orderBy(F.col("vintage_date").asc())

    recomputed = (
        spark.table(table)
        .withColumn("_previous", F.lag("value").over(by_vintage))
        .withColumn(
            "_latest_vintage",
            F.max("vintage_date").over(Window.partitionBy("series_id", "period")),
        )
        .withColumn("is_latest", F.col("vintage_date") == F.col("_latest_vintage"))
        .withColumn(
            "revision_delta",
            # Null when there is no previous vintage, and null when either side is missing.
            # "Not yet revised" and "revised by zero" are different facts; so are "revised to
            # nothing" and "revised by the whole value".
            F.when(
                F.col("_previous").isNotNull() & F.col("value").isNotNull(),
                F.col("value") - F.col("_previous"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("_previous", "_latest_vintage")
        .localCheckpoint(eager=True)
    )

    revisions = recomputed.where(
        F.col("revision_delta").isNotNull() & (F.col("revision_delta") != 0)
    ).count()
    recomputed.writeTo(table).overwritePartitions()
    return revisions


def macro_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    bronze_table: str = BRONZE_TABLE,
    macro_table: str = MACRO_TABLE,
) -> MacroResult:
    """Bronze window, `macro` partitions only -> `gold.macro_observations`, MERGEd."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ensure_table(spark, macro_table)

    windowed = _bronze_window(spark, since, until, table=bronze_table)
    bronze_rows = windowed.where(F.col("outcome") == "ok").count()

    extracted = windowed.where(F.col("outcome") == "ok").mapInPandas(
        _extract_partitions, schema=MACRO_ROW_SCHEMA
    )

    # Every fetch re-states the full bounded history, so consecutive fetches inside one
    # window carry the same vintages. Keep the newest observation of each — identical in
    # value by construction, but the later `observed_at` is the honest one.
    newest = Window.partitionBy("series_id", "period", "vintage_date").orderBy(
        F.col("observed_at").desc()
    )
    extracted = (
        extracted.withColumn("_rank", F.row_number().over(newest))
        .where(F.col("_rank") == 1)
        .drop("_rank")
        .select(*EXTRACT_COLUMNS)
        .localCheckpoint(eager=True)
    )
    observations_extracted = extracted.count()
    series_seen = extracted.select("series_id").distinct().count()

    before = spark.table(macro_table).count()
    if observations_extracted:
        extracted.createOrReplaceTempView("_incoming_macro")
        # No UPDATE branch: a published vintage never changes, so a matched row is a re-read
        # of a fact already recorded and the correct action is nothing. `is_latest` is a
        # placeholder here and is overwritten by `recompute_derived` immediately below —
        # the column is NOT NULL, so it cannot simply be left out of the insert.
        spark.sql(
            f"""
            MERGE INTO {macro_table} t
            USING _incoming_macro s
            ON t.series_id = s.series_id
               AND t.period = s.period
               AND t.vintage_date = s.vintage_date
            WHEN NOT MATCHED THEN INSERT (
                series_id, period, value, vintage_date, superseded_at,
                is_latest, revision_delta, observed_at, ingest_id
            ) VALUES (
                s.series_id, s.period, s.value, s.vintage_date, s.superseded_at,
                false, NULL, s.observed_at, s.ingest_id
            )
            """
        )

    after = spark.table(macro_table).count()
    revisions = recompute_derived(spark, macro_table) if after else 0

    return MacroResult(
        bronze_rows=bronze_rows,
        observations_extracted=observations_extracted,
        observations_committed=after - before,
        series_seen=series_seen,
        revisions_found=revisions,
        table_rows=after,
    )
