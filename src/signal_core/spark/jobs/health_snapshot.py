"""`ops.source_health`, one row per source per window. SPEC §9, §11, §6.3.

The monitoring in this project is data, not a dashboard: the brief's footer, the alerting
in Airflow, and any "was the pipeline healthy last Tuesday?" question all read the same
table. That is also why it is written with a MERGE keyed on `(source_id, window_start)` —
re-running the monitoring DAG over an interval has to correct the row rather than
duplicate it, or the history it produces cannot be trusted to answer the question.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING

from signal_core.ops.health import SourceHealth

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

OPS_NAMESPACE = "ops"
HEALTH_TABLE = "ops.source_health"

HEALTH_DDL = """
    source_id string NOT NULL,
    window_start timestamp NOT NULL,
    docs_ingested int,
    expected_min int,
    last_success_at timestamp,
    staleness_seconds double,
    gap_reason string,
    status string,
    content_staleness_seconds double,
    baseline_docs double
"""

# Same columns without the constraints: `NOT NULL` is table DDL, and Spark's DataFrame
# schema parser rejects it.
HEALTH_SCHEMA = """
    source_id string, window_start timestamp, docs_ingested int, expected_min int,
    last_success_at timestamp, staleness_seconds double, gap_reason string, status string,
    content_staleness_seconds double, baseline_docs double
"""

# Columns added after the table was first deployed. `CREATE TABLE IF NOT EXISTS` is a
# no-op against the live table in AWS, so a new column in HEALTH_DDL would never reach it
# and the MERGE below would fail on a column the target doesn't have. Iceberg's schema
# evolution is metadata-only — no data rewrite — so this is cheap and idempotent; existing
# rows read back NULL for the new columns, which is the truth about windows assessed
# before these signals existed.
_ADDED_COLUMNS = (
    ("content_staleness_seconds", "double"),
    ("baseline_docs", "double"),
)


def ensure_table(spark: SparkSession, table: str = HEALTH_TABLE) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({HEALTH_DDL})
        USING iceberg
        PARTITIONED BY (months(window_start))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    # Spark SQL has no `ADD COLUMN IF NOT EXISTS`, so the existing schema is what makes
    # this idempotent — re-adding a present column is an AnalysisException, not a no-op.
    existing = set(spark.table(table).columns)
    for column, sql_type in _ADDED_COLUMNS:
        if column not in existing:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")


def record(
    spark: SparkSession,
    healths: Iterable[SourceHealth],
    window_start: datetime,
    *,
    table: str = HEALTH_TABLE,
) -> int:
    """Upsert one row per source for `window_start`. Returns rows written."""
    ensure_table(spark, table)

    rows = [
        (
            h.source_id,
            window_start,
            h.docs_ingested,
            h.expected_min,
            h.last_success_at,
            # inf is honest in Python and unrepresentable in a Parquet double column that
            # anyone will later average; "never succeeded" is already in `status`.
            None if h.staleness_seconds == float("inf") else float(h.staleness_seconds),
            h.gap_reason,
            h.status,
            # None here means "this source has no content-movement signal", which is a
            # different fact from "content is fresh" — the column stays NULL rather than
            # inventing a zero that would read as the latter.
            None if h.content_staleness_seconds is None else float(h.content_staleness_seconds),
            None if h.baseline_docs is None else float(h.baseline_docs),
        )
        for h in healths
    ]
    if not rows:
        return 0

    spark.createDataFrame(rows, schema=HEALTH_SCHEMA).createOrReplaceTempView("health_rows")
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING health_rows AS source
        ON target.source_id = source.source_id AND target.window_start = source.window_start
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return len(rows)
