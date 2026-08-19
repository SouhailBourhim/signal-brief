"""`ops.pipeline_costs`. SPEC §9, §10.3, §17.

Same Hadoop-catalog-over-a-temp-warehouse shape as `test_commit_bronze.py` and
`test_normalize_window.py`.
"""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.spark

TABLE = "ops.pipeline_costs"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-costs", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_table(spark):
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    yield


def test_record_writes_one_row_per_record(spark):
    from signal_core.spark.jobs.cost_snapshot import CostRecord, record

    written = record(
        spark,
        [
            CostRecord(
                run_id="run-1",
                dag_id="ingest_monitor",
                task_id="commit_staged",
                run_date=date(2026, 8, 19),
                s3_egress_bytes=239_000,
            ),
            CostRecord(
                run_id="run-1",
                dag_id="process",
                task_id="athena_smoke_query",
                run_date=date(2026, 8, 19),
                bytes_scanned=52_428_800,
                athena_cost_usd=0.00025,
            ),
        ],
    )
    assert written == 2
    assert spark.table(TABLE).count() == 2


def test_optional_fields_missing_a_measurement_stay_null(spark):
    """SPEC §17: a task that only measured egress must not have to fabricate
    `bytes_scanned` just to write a row."""
    from signal_core.spark.jobs.cost_snapshot import CostRecord, record

    record(
        spark,
        [
            CostRecord(
                run_id="run-1",
                dag_id="ingest_monitor",
                task_id="commit_staged",
                run_date=date(2026, 8, 19),
                s3_egress_bytes=100,
            )
        ],
    )
    row = spark.table(TABLE).collect()[0]
    assert row.s3_egress_bytes == 100
    assert row.bytes_scanned is None
    assert row.athena_cost_usd is None
    assert row.lambda_ms is None
    assert row.s3_requests is None


def test_replay_of_the_same_run_dag_task_corrects_rather_than_duplicates(spark):
    """Unlike `silver.articles`'s insert-only MERGE, a cost record is corrected on
    re-run, the same reasoning `health_snapshot.record` already applies to
    `ops.source_health`: it describes a fact about one run, not an immutable event."""
    from signal_core.spark.jobs.cost_snapshot import CostRecord, record

    record(
        spark,
        [
            CostRecord(
                run_id="run-1",
                dag_id="ingest_monitor",
                task_id="commit_staged",
                run_date=date(2026, 8, 19),
                s3_egress_bytes=100,
            )
        ],
    )
    record(
        spark,
        [
            CostRecord(
                run_id="run-1",
                dag_id="ingest_monitor",
                task_id="commit_staged",
                run_date=date(2026, 8, 19),
                s3_egress_bytes=999,  # corrected number
            )
        ],
    )

    rows = spark.table(TABLE).collect()
    assert len(rows) == 1
    assert rows[0].s3_egress_bytes == 999


def test_record_with_no_records_is_a_no_op(spark):
    from signal_core.spark.jobs.cost_snapshot import ensure_table, record

    ensure_table(spark)
    assert record(spark, []) == 0
    assert spark.table(TABLE).count() == 0


def test_table_is_partitioned_by_month(spark):
    from signal_core.spark.jobs.cost_snapshot import CostRecord, record

    record(
        spark,
        [
            CostRecord(
                run_id="run-1",
                dag_id="ingest_monitor",
                task_id="commit_staged",
                run_date=date(2026, 8, 19),
                s3_egress_bytes=1,
            )
        ],
    )
    partitions = spark.sql(f"SELECT partition FROM {TABLE}.partitions").collect()
    assert hasattr(partitions[0].partition, "run_date_month")
