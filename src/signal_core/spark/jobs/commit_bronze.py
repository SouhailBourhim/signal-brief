"""Staging -> `bronze.raw_documents`, on Spark. SPEC §6.4, §6.2, §12.

The poller Lambdas land gzipped JSONL in a staging prefix (`signal_core/staging.py`).
This job is the other half: it reads a staged interval, decodes payloads back to bytes,
and commits them into the Iceberg table that is the project's immutable record.

Two properties matter more than throughput here, because SPEC §12's Phase 1 acceptance
test is exactly a test of them:

  * **No duplicates.** The commit is a MERGE on `ingest_id`, so re-running it over an
    interval already committed inserts nothing. That is what makes replay safe to run
    repeatedly — and it has to be, because the recovery procedure after downtime is "run
    it again over a wider window".
  * **No gaps.** Nothing is filtered on the way in. A fetch that failed is a row with
    `outcome=ERROR` and the error text as its payload (SPEC §6.2: quarantined with a
    reason, never silently dropped), so a hole in `bronze.raw_documents` means a hole in
    what was actually collected, not a hole in what was committed.

Staged objects are left in place; the bucket's lifecycle rule expires them (see
infra/terraform/main). Deleting them here would make the job's own retry unsafe.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE = "bronze.raw_documents"

# Explicit, never inferred: schema inference reads every staged object twice and would
# happily retype a column the day a source emits an all-null batch.
STAGING_SCHEMA = """
    ingest_id string, source_id string, fetched_at string, source_url string,
    http_status int, outcome string, etag string, last_modified string,
    content_hash string, payload_b64 string, payload_format string,
    latency_ms int, byte_count int
"""

BRONZE_DDL = """
    ingest_id string NOT NULL,
    source_id string NOT NULL,
    fetched_at timestamp NOT NULL,
    ingest_date date NOT NULL,
    source_url string,
    http_status int,
    outcome string,
    etag string,
    last_modified string,
    content_hash string,
    payload binary,
    payload_format string,
    latency_ms int,
    byte_count int
"""


@dataclass(frozen=True)
class CommitResult:
    """What a commit did, for the health record SPEC §11 builds its monitoring from."""

    staged_rows: int
    committed_rows: int
    table_rows: int

    @property
    def duplicate_rows(self) -> int:
        """Staged rows the MERGE already had. Nonzero is normal on a replay — and is the
        number the Phase 1 acceptance test asserts is *entirely* what a replay produces."""
        return self.staged_rows - self.committed_rows


def ensure_table(spark: SparkSession, table: str = BRONZE_TABLE) -> None:
    """Create the table if it isn't there. Partitioned by `(source_id, ingest_date)`
    exactly as SPEC §6.4 specifies, which is also the staging layout — so a backfill of
    one source-day rewrites one partition and reads no others."""
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({BRONZE_DDL})
        USING iceberg
        PARTITIONED BY (source_id, ingest_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            -- Payloads are whole documents; small files here are the compaction problem
            -- SPEC §12's Phase 4 measures, so start with a sane target.
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def read_staged(
    spark: SparkSession,
    staging_root: str | Path,
    *,
    source_id: str | None = None,
    ingest_date: str | None = None,
) -> DataFrame:
    """Read a staged interval into bronze's schema.

    `source_id` / `ingest_date` narrow the read to one partition path rather than
    filtering after the fact — on S3 that is the difference between listing one prefix
    and listing the bucket, which SPEC §10.1 counts in requests and egress.
    """
    from pyspark.sql import functions as F

    root = str(staging_root).rstrip("/")
    path = f"{root}/source={source_id}" if source_id else f"{root}/source=*"
    path = f"{path}/ingest_date={ingest_date}" if ingest_date else f"{path}/ingest_date=*"
    staged = spark.read.schema(STAGING_SCHEMA).json(f"{path}/hour=*/*.jsonl.gz")

    fetched_at = F.col("fetched_at").cast("timestamp")
    return staged.select(
        "ingest_id",
        "source_id",
        fetched_at.alias("fetched_at"),
        F.to_date(fetched_at).alias("ingest_date"),
        "source_url",
        "http_status",
        "outcome",
        "etag",
        "last_modified",
        "content_hash",
        # Base64 in, bytes out: staging never decoded the payload, and neither does this
        # (SPEC §6.1 — interpretation happens downstream, against these stored bytes).
        F.unbase64(F.col("payload_b64")).alias("payload"),
        "payload_format",
        "latency_ms",
        "byte_count",
    )


def commit(
    spark: SparkSession,
    staging_root: str | Path,
    *,
    table: str = BRONZE_TABLE,
    source_id: str | None = None,
    ingest_date: str | None = None,
) -> CommitResult:
    """Merge a staged interval into `table`. Idempotent on `ingest_id`."""
    ensure_table(spark, table)
    staged = read_staged(spark, staging_root, source_id=source_id, ingest_date=ingest_date)

    # Within-batch duplicates would slip past the MERGE's NOT MATCHED clause, which only
    # sees the target. An overlapping catch-up window stages the same document twice, so
    # this is a real case, not a defensive one.
    staged = staged.dropDuplicates(["ingest_id"])
    staged.createOrReplaceTempView("staged_documents")

    before = spark.table(table).count()
    staged_rows = spark.table("staged_documents").count()
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING staged_documents AS source
        ON target.ingest_id = source.ingest_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table(table).count()
    return CommitResult(staged_rows=staged_rows, committed_rows=after - before, table_rows=after)
