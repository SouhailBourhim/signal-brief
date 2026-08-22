"""Iceberg table maintenance. SPEC §11, §12's 4A acceptance ("compaction delta measured").

Three operations per table, nightly:

- **`rewrite_data_files`** — compaction. Every writer in this pipeline commits small files:
  `commit_bronze` runs hourly, `normalize` per window, `hn_scores` lands ~240 documents an
  hour. Left alone, a query's planning cost grows with the file count rather than the data.
- **`expire_snapshots`** — Iceberg keeps every snapshot forever by default, and each one
  pins the data files it referenced. Without this, compaction *increases* storage: the new
  compacted file lands and the old fragments stay reachable.
- **`remove_orphan_files`** — files no snapshot references, left by failed or interrupted
  writes. 3.B's concurrent-writer incident is exactly the shape that produces these.

`ops.maintenance_runs` records before/after file counts per table, which is what SPEC §12's
4A acceptance means by a measured delta — a number, from a real run, not a green test.

## What the pinned Iceberg actually accepts

Verified against this repo's Spark 4.1 / Iceberg pin (ADR-0006) rather than taken from the
docs, and two of the three needed correcting from the obvious form:

- **`remove_orphan_files` refuses any `older_than` under 24 hours**, with
  `IllegalArgumentException: Cannot remove orphan files with an interval less than 24 hours`.
  That is a corruption guard — a shorter interval can delete files an in-flight write is
  about to commit — and it happens to match what this job wants anyway, so it is respected
  rather than overridden.
- **`rewrite_data_files` silently no-ops below `min-input-files`** (default 5), returning
  `rewritten_data_files_count=0`. Not an error, and easy to mistake for "nothing to do" when
  the real answer is "not enough fragments yet". The parameter is exposed so a test can force
  a rewrite and assert a real delta.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from signal_core.spark.tables import ensure_columns
from signal_core.timeutil import utc_now

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

MAINTENANCE_TABLE = "ops.maintenance_runs"

# Every table this pipeline writes. Listed rather than discovered, because a table missing
# from a maintenance sweep degrades quietly — queries get slower and storage grows, with no
# failure anywhere — and a hardcoded list is the thing a reviewer can check against the DDLs.
MAINTAINED_TABLES: tuple[str, ...] = (
    "bronze.raw_documents",
    "silver.articles",
    "silver.hn_comments",
    "silver.hn_score_snapshots",
    "silver.market_observations",
    "silver.parse_rejects",
    "silver.story_clusters",
    "silver.article_clusters",
    "silver.entity_mentions",
    "silver.dim_entities",
    "ops.source_health",
    "ops.pipeline_costs",
    # The gold marts. Written through Athena rather than Spark (`brief/items.py`,
    # `enrich/store.py`), which is exactly why they were missed until 4B:
    # `test_every_maintained_table_is_one_the_pipeline_actually_writes` walks the DDL
    # constants in `spark/jobs/`, so a table no Spark job writes could never fail it.
    # `gold.brief_items` has been written since 4A and swept by nothing. They are Iceberg
    # tables in the same Glue catalog, so the procedures reach them the same way.
    "gold.brief_items",
    "gold.cluster_enrichment",
    "gold.enrichment_rejects",
    "gold.macro_observations",
)

MAINTENANCE_DDL = """
    run_id string NOT NULL,
    table_name string NOT NULL,
    run_date date NOT NULL,
    files_before int,
    files_after int,
    rewritten_files int,
    added_files int,
    rewritten_bytes bigint,
    deleted_manifests int,
    orphans_removed int,
    error string,
    skipped string
"""

MAINTENANCE_SCHEMA = """
    run_id string, table_name string, run_date date, files_before int, files_after int,
    rewritten_files int, added_files int, rewritten_bytes bigint, deleted_manifests int,
    orphans_removed int, error string, skipped string
"""

# How much history to keep. Seven days of snapshots is enough to answer "what did this table
# look like on Monday" — Iceberg time travel is the cheapest debugging tool in the stack, and
# 3.B's duplicate-row incident was diagnosed by reading an older snapshot.
SNAPSHOT_RETENTION_DAYS = 7
# Never below Iceberg's own 24-hour floor; see the module docstring.
ORPHAN_RETENTION_DAYS = 7
# Iceberg's default. Below this, compaction is not worth the rewrite.
MIN_INPUT_FILES = 5

# `remove_orphan_files` is the one procedure here that does not go through Iceberg's own
# `S3FileIO`. Finding files no snapshot references means *listing the table's directory*, and
# that goes through Hadoop's `FileSystem` API — which has no handler for `s3://` (only
# `s3a://`), and in this session has no S3 handler at all: `spark/session.py` ships
# `iceberg-aws-bundle`, which is Iceberg's AWS integration, not Hadoop's `hadoop-aws`.
# Verified against the deployed warehouse on 2026-08-22: `S3AFileSystem` is absent from the
# classpath, and every table failed with
# `UnsupportedFileSystemException: No FileSystem for scheme "s3"`.
#
# **Adding `hadoop-aws` is not obviously the right trade.** It pulls an AWS SDK that has to
# agree with the one in `iceberg-aws-bundle`, which is exactly the jar-version class of
# failure ADR-0006 exists to prevent — and it would buy the reclamation of files left by
# interrupted writes on a lake whose storage sits inside the free tier. Compaction and
# snapshot expiry, the two operations that actually govern query cost and storage growth,
# both work.
#
# So this is recorded as a **skip**, not an error: the nightly DAG stays green on a known
# and documented limitation rather than failing every night, while `ops.maintenance_runs`
# still carries the reason per table. If `hadoop-aws` ever lands for another reason, this
# starts working with no change here — the detection is on the behaviour, not on the config.
_NO_HADOOP_FS = (
    "No FileSystem for scheme",
    "UnsupportedFileSystemException",
)


@dataclass(frozen=True)
class TableMaintenance:
    table: str
    files_before: int = 0
    files_after: int = 0
    rewritten_files: int = 0
    added_files: int = 0
    rewritten_bytes: int = 0
    deleted_manifests: int = 0
    orphans_removed: int = 0
    error: str | None = None
    skipped: str | None = None

    @property
    def delta(self) -> int:
        """Files removed by compaction. The acceptance criterion's number."""
        return self.files_before - self.files_after


@dataclass(frozen=True)
class MaintenanceResult:
    tables: list[TableMaintenance]

    @property
    def files_before(self) -> int:
        return sum(t.files_before for t in self.tables)

    @property
    def files_after(self) -> int:
        return sum(t.files_after for t in self.tables)

    @property
    def delta(self) -> int:
        return self.files_before - self.files_after

    @property
    def failed(self) -> list[str]:
        return [t.table for t in self.tables if t.error]

    @property
    def skipped(self) -> list[str]:
        """Operations that could not run for a known, recorded reason — distinct from
        `failed`, which is a fault. Reported so a permanent skip stays visible rather than
        becoming invisible by being green (SPEC §11)."""
        return [t.table for t in self.tables if t.skipped]


def ensure_table(spark: SparkSession, table: str = MAINTENANCE_TABLE) -> list[str]:
    """Create the record table, and add any column it is missing. Returns what was added.

    The `ensure_columns` call is not decoration. `CREATE TABLE IF NOT EXISTS` is a no-op
    against a live table, so a column added to `MAINTENANCE_DDL` never reaches a deployed
    one — and this table caught that on its own second run: the first real sweep created it
    with eleven columns, `skipped` was added an hour later, and reading the new column back
    failed against the deployed table while every test passed.

    That is 3.D's finding exactly (*a deployed table two columns behind its own DDL,
    discovered only when the brief failed reading them*), recurring in the table written to
    record maintenance. Returned rather than logged for the reason `spark/tables.py` gives:
    a schema that just changed under a running pipeline is something a person should see.
    """
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({MAINTENANCE_DDL})
        USING iceberg
        PARTITIONED BY (months(run_date))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    return ensure_columns(spark, table, MAINTENANCE_DDL)


def _catalog_of(spark: SparkSession) -> str:
    """Which catalog the procedures live under.

    `CALL` is namespaced by catalog — `CALL <catalog>.system.rewrite_data_files(...)` — so
    this cannot be hardcoded: `build_iceberg_session` names the catalog from settings, and
    tests use `test` while the deployed session uses `signal`.
    """
    return spark.conf.get("spark.sql.defaultCatalog") or "spark_catalog"


def _file_count(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}.files").collect()[0]["n"]


def _table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
        return True
    except Exception:
        return False


def maintain_table(
    spark: SparkSession,
    table: str,
    *,
    now: datetime | None = None,
    min_input_files: int = MIN_INPUT_FILES,
) -> TableMaintenance:
    """Compact, expire, de-orphan one table. Never raises — see below.

    A failure on one table must not stop the sweep: the tables are independent, and a
    maintenance job that abandons nine tables because the tenth is locked has made the
    problem worse. The error is recorded per table and the DAG fails on the aggregate, so it
    is visible without being fatal mid-sweep.
    """
    now = now or utc_now()
    catalog = _catalog_of(spark)
    # `CALL` takes the identifier *without* the catalog prefix, since the catalog is already
    # named by the procedure path — `CALL cat.system.f(table => 'db.tbl')`.
    identifier = table

    try:
        before = _file_count(spark, table)
    except Exception as exc:
        return TableMaintenance(table=table, error=f"unreadable: {exc}"[:500])

    rewritten = added = rewritten_bytes = deleted_manifests = orphans = 0
    error: str | None = None
    skipped: str | None = None

    # Three separate attempts, not one block. They are independent operations and the third
    # is known-unavailable on the deployed warehouse (see `_NO_HADOOP_FS`), so sharing a
    # `try` would let a documented limitation discard two successful results.
    try:
        row = spark.sql(
            f"CALL {catalog}.system.rewrite_data_files("
            f"table => '{identifier}', "
            f"options => map('min-input-files','{min_input_files}'))"
        ).collect()[0]
        rewritten = row["rewritten_data_files_count"]
        added = row["added_data_files_count"]
        rewritten_bytes = row["rewritten_bytes_count"]
    except Exception as exc:
        error = f"rewrite_data_files: {type(exc).__name__}: {exc}"[:500]

    try:
        expire_before = (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        expired = spark.sql(
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{identifier}', "
            f"older_than => TIMESTAMP '{expire_before}', "
            f"retain_last => 5)"
        ).collect()[0]
        deleted_manifests = expired["deleted_manifest_lists_count"]
    except Exception as exc:
        error = error or f"expire_snapshots: {type(exc).__name__}: {exc}"[:500]

    try:
        orphan_before = (now - timedelta(days=ORPHAN_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        orphan_rows = spark.sql(
            f"CALL {catalog}.system.remove_orphan_files("
            f"table => '{identifier}', "
            f"older_than => TIMESTAMP '{orphan_before}')"
        ).collect()
        orphans = len(orphan_rows)
    except Exception as exc:
        message = str(exc)
        if any(marker in message for marker in _NO_HADOOP_FS):
            # Recorded as a skip, not a failure. See `_NO_HADOOP_FS`.
            skipped = "remove_orphan_files: no Hadoop filesystem for the warehouse scheme"
        else:
            error = error or f"remove_orphan_files: {type(exc).__name__}: {exc}"[:500]

    try:
        after = _file_count(spark, table)
    except Exception as exc:
        after = before
        error = error or f"unreadable after maintenance: {exc}"[:500]

    return TableMaintenance(
        table=table,
        files_before=before,
        files_after=after,
        rewritten_files=rewritten,
        added_files=added,
        rewritten_bytes=rewritten_bytes,
        deleted_manifests=deleted_manifests,
        orphans_removed=orphans,
        error=error,
        skipped=skipped,
    )


def maintain(
    spark: SparkSession,
    *,
    tables: tuple[str, ...] = MAINTAINED_TABLES,
    run_id: str | None = None,
    now: datetime | None = None,
    record_table: str = MAINTENANCE_TABLE,
    min_input_files: int = MIN_INPUT_FILES,
) -> MaintenanceResult:
    """Sweep every table, record the deltas, return them."""
    now = now or utc_now()
    run_id = run_id or now.strftime("%Y%m%dT%H%M%S")

    # A table that has never been created is skipped rather than recorded as an error: a
    # fresh environment legitimately has no `silver.market_observations` until the market DAG
    # first runs, and a nightly job that reports failures for tables nobody has built yet is
    # one whose failures stop being read (SPEC §11).
    present = [t for t in tables if _table_exists(spark, t)]
    results = [
        maintain_table(spark, table, now=now, min_input_files=min_input_files) for table in present
    ]

    ensure_table(spark, record_table)
    if results:
        rows = [
            (
                run_id,
                r.table,
                now.date(),
                r.files_before,
                r.files_after,
                r.rewritten_files,
                r.added_files,
                r.rewritten_bytes,
                r.deleted_manifests,
                r.orphans_removed,
                r.error,
                r.skipped,
            )
            for r in results
        ]
        spark.createDataFrame(rows, schema=MAINTENANCE_SCHEMA).createOrReplaceTempView(
            "_maintenance_rows"
        )
        spark.sql(
            f"""
            MERGE INTO {record_table} AS target
            USING _maintenance_rows AS source
            ON target.run_id = source.run_id AND target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    return MaintenanceResult(tables=results)
