"""Iceberg maintenance. 4A.F; SPEC §11, §12's "compaction delta measured".

The procedure forms here were verified against this repo's actual Spark/Iceberg pin before
the job was written (ADR-0006), and two of the three needed correcting from the obvious
form — see `spark/jobs/maintain.py`'s docstring. These tests pin both corrections.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

TABLE = "probe.fragmented"
RECORD_TABLE = "ops.maintenance_runs"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-maintain", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean(spark):
    for table in (TABLE, RECORD_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    yield


def _fragment(spark, table: str = TABLE, commits: int = 6) -> None:
    """One tiny file per commit — the shape every writer in this pipeline produces."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (id int, s string) USING iceberg")
    for i in range(commits):
        spark.sql(f"INSERT INTO {table} VALUES ({i}, 'row{i}')")


def _files(spark, table: str = TABLE) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}.files").collect()[0]["n"]


def test_compaction_actually_reduces_the_file_count(spark):
    """The acceptance criterion is a measured delta, so the test measures one rather than
    asserting the procedure was called."""
    from signal_core.spark.jobs.maintain import maintain_table

    _fragment(spark, commits=6)
    assert _files(spark) == 6

    result = maintain_table(spark, TABLE, min_input_files=2)

    assert result.error is None
    assert result.files_before == 6
    assert result.files_after < result.files_before
    assert result.delta > 0
    assert result.rewritten_files == 6
    assert result.rewritten_bytes > 0


def test_below_min_input_files_compaction_is_a_no_op_not_an_error(spark):
    """Iceberg's `rewrite_data_files` returns `rewritten_data_files_count=0` rather than
    failing when there are too few fragments to be worth rewriting. Easy to mistake for
    "nothing to do" when the real answer is "not enough fragments yet", which is why the
    threshold is a named parameter instead of an implicit default."""
    from signal_core.spark.jobs.maintain import maintain_table

    _fragment(spark, commits=3)
    result = maintain_table(spark, TABLE, min_input_files=10)

    assert result.error is None
    assert result.rewritten_files == 0
    assert result.files_after == result.files_before


def test_orphan_removal_respects_icebergs_24_hour_floor(spark):
    """Iceberg refuses `older_than` under 24 hours —
    "Cannot remove orphan files with an interval less than 24 hours" — because a shorter
    interval can delete files an in-flight write is about to commit. The job uses 7 days,
    so it clears the floor rather than overriding it."""
    from signal_core.spark.jobs import maintain as maintain_module

    assert maintain_module.ORPHAN_RETENTION_DAYS >= 1

    _fragment(spark, commits=6)
    result = maintain_module.maintain_table(spark, TABLE, min_input_files=2)
    assert result.error is None, "a sub-24h interval would have raised here"


def test_a_missing_table_is_skipped_not_recorded_as_a_failure(spark):
    """A fresh environment has no `silver.market_observations` until the market DAG first
    runs. A nightly job reporting failures for tables nobody has built yet is one whose
    failures stop being read (SPEC §11)."""
    from signal_core.spark.jobs.maintain import maintain

    _fragment(spark, commits=6)
    result = maintain(spark, tables=(TABLE, "silver.does_not_exist"), min_input_files=2)

    assert [t.table for t in result.tables] == [TABLE]
    assert result.failed == []


def test_one_broken_table_does_not_abandon_the_others(spark):
    """The sweep is over independent tables; giving up on nine because the tenth is locked
    makes the problem worse."""
    from signal_core.spark.jobs import maintain as maintain_module

    _fragment(spark, commits=6)

    real_count = maintain_module._file_count

    def _explode(session, table):
        if table == TABLE:
            raise RuntimeError("boom")
        return real_count(session, table)

    second = "probe.other"
    _fragment(spark, table=second, commits=6)
    maintain_module._file_count = _explode
    try:
        result = maintain_module.maintain(spark, tables=(TABLE, second), min_input_files=2)
    finally:
        maintain_module._file_count = real_count

    assert result.failed == [TABLE]
    healthy = next(t for t in result.tables if t.table == second)
    assert healthy.error is None
    assert healthy.delta > 0


def test_the_run_is_recorded_with_before_and_after_counts(spark):
    """`ops.maintenance_runs` is where SPEC §12's measured delta lives — a table, not a log
    line, so "what did compaction buy last Tuesday" is a query."""
    from signal_core.spark.jobs.maintain import maintain

    _fragment(spark, commits=6)
    maintain(spark, tables=(TABLE,), run_id="run-1", min_input_files=2)

    row = spark.table(RECORD_TABLE).collect()[0]
    assert row.run_id == "run-1"
    assert row.table_name == TABLE
    assert row.files_before == 6
    assert row.files_after < row.files_before
    assert row.error is None


def test_rerunning_the_same_run_id_updates_rather_than_duplicating(spark):
    """MERGE on `(run_id, table_name)`, matching `cost_snapshot.record`: a corrected number
    should overwrite the old one rather than sit beside it."""
    from signal_core.spark.jobs.maintain import maintain

    _fragment(spark, commits=6)
    maintain(spark, tables=(TABLE,), run_id="run-1", min_input_files=2)
    maintain(spark, tables=(TABLE,), run_id="run-1", min_input_files=2)

    assert spark.table(RECORD_TABLE).count() == 1


def test_a_record_table_created_before_a_column_existed_gains_it(spark):
    """3.D's defect, caught here on this table's own second real run.

    `CREATE TABLE IF NOT EXISTS` is a no-op against a live table, so a column added to the
    DDL never reaches a deployed one. The first sweep against the real lake created
    `ops.maintenance_runs` with eleven columns; `skipped` was added afterwards; reading it
    back failed against the deployed table while every test still passed — because the tests
    always created the table fresh, from the current DDL.

    This one does not: it creates the *old* shape first, the way a deployed table already
    exists, and asserts `ensure_table` migrates it.
    """
    from signal_core.spark.jobs.maintain import MAINTENANCE_DDL, ensure_table

    older_ddl = "\n".join(
        line for line in MAINTENANCE_DDL.strip().splitlines() if "skipped" not in line
    ).rstrip(",")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ops")
    spark.sql(f"CREATE TABLE {RECORD_TABLE} ({older_ddl}) USING iceberg")
    assert "skipped" not in {f.name for f in spark.table(RECORD_TABLE).schema.fields}

    added = ensure_table(spark)

    assert added == ["skipped"], "the missing column should be added and reported"
    assert "skipped" in {f.name for f in spark.table(RECORD_TABLE).schema.fields}


def test_every_maintained_table_is_one_the_pipeline_actually_writes(spark):
    """A hardcoded list is the thing a reviewer can check; this asserts it has not drifted
    from the DDL constants the jobs use."""
    from signal_core.spark.jobs.cluster import ARTICLE_CLUSTERS_TABLE, CLUSTERS_TABLE
    from signal_core.spark.jobs.commit_bronze import BRONZE_TABLE
    from signal_core.spark.jobs.cost_snapshot import COSTS_TABLE
    from signal_core.spark.jobs.maintain import MAINTAINED_TABLES
    from signal_core.spark.jobs.market import MARKET_TABLE
    from signal_core.spark.jobs.normalize import (
        ARTICLES_TABLE,
        HN_COMMENTS_TABLE,
        HN_SCORES_TABLE,
        PARSE_REJECTS_TABLE,
    )

    for table in (
        BRONZE_TABLE,
        ARTICLES_TABLE,
        HN_COMMENTS_TABLE,
        HN_SCORES_TABLE,
        PARSE_REJECTS_TABLE,
        MARKET_TABLE,
        CLUSTERS_TABLE,
        ARTICLE_CLUSTERS_TABLE,
        COSTS_TABLE,
    ):
        assert table in MAINTAINED_TABLES, f"{table} is written but never maintained"
