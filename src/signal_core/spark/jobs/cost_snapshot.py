"""`ops.pipeline_costs`, one row per `(run_id, dag_id, task_id)`. SPEC §9, §10.3, §17.

Modeled directly on `health_snapshot.py`: monitoring here is data a DAG writes, not a
dashboard, so "what did last Tuesday's run cost?" is answerable by querying a table
rather than reconstructing it from CloudWatch and memory. MERGE keyed on the same triple
a re-run would reuse, `UPDATE SET *` on match — a corrected number should overwrite the
old one, the same argument `health_snapshot.record` already makes for `source_health`.

Every numeric field is optional: one task measures Athena bytes scanned, another measures
S3 egress, and neither should have to fabricate the field it didn't measure just to write
a row (SPEC §17 — a metric this pipeline can't recompute doesn't get claimed at all).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

COSTS_TABLE = "ops.pipeline_costs"

COSTS_DDL = """
    run_id string NOT NULL,
    dag_id string NOT NULL,
    task_id string NOT NULL,
    bytes_scanned bigint,
    athena_cost_usd double,
    lambda_ms bigint,
    s3_requests bigint,
    s3_egress_bytes bigint,
    run_date date NOT NULL
"""

# Same columns, no constraints: Spark's DataFrame schema parser rejects `NOT NULL`.
COSTS_SCHEMA = """
    run_id string, dag_id string, task_id string, bytes_scanned bigint,
    athena_cost_usd double, lambda_ms bigint, s3_requests bigint,
    s3_egress_bytes bigint, run_date date
"""


@dataclass(frozen=True)
class CostRecord:
    run_id: str
    dag_id: str
    task_id: str
    run_date: date
    bytes_scanned: int | None = None
    athena_cost_usd: float | None = None
    lambda_ms: int | None = None
    s3_requests: int | None = None
    s3_egress_bytes: int | None = None


def ensure_table(spark: SparkSession, table: str = COSTS_TABLE) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({COSTS_DDL})
        USING iceberg
        PARTITIONED BY (months(run_date))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def record(
    spark: SparkSession,
    records: Iterable[CostRecord],
    *,
    table: str = COSTS_TABLE,
) -> int:
    """Upsert one row per `(run_id, dag_id, task_id)`. Returns rows written."""
    ensure_table(spark, table)

    rows = [
        (
            r.run_id,
            r.dag_id,
            r.task_id,
            r.bytes_scanned,
            r.athena_cost_usd,
            r.lambda_ms,
            r.s3_requests,
            r.s3_egress_bytes,
            r.run_date,
        )
        for r in records
    ]
    if not rows:
        return 0

    spark.createDataFrame(rows, schema=COSTS_SCHEMA).createOrReplaceTempView("cost_rows")
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING cost_rows AS source
        ON target.run_id = source.run_id
           AND target.dag_id = source.dag_id
           AND target.task_id = source.task_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return len(rows)
