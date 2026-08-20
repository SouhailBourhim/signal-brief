"""`ops.source_health` as a table. SPEC §9, §11."""

from __future__ import annotations

from datetime import timedelta

import pytest

from signal_core.config import SOURCES
from signal_core.contracts import State
from signal_core.ops.monitor import assess
from signal_core.timeutil import utc_now

pytestmark = pytest.mark.spark

TABLE = "ops.source_health"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    session = build_iceberg_session(
        "signal-health-test", warehouse=tmp_path_factory.mktemp("warehouse"), catalog="test"
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_table(spark):
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    yield


def _verdicts(now):
    return [
        assess(
            SOURCES["hackernews"],
            # Fetching seconds ago and producing at its normal rate: hackernews runs
            # 119-919 documents an hour in production, so 600 is an ordinary window and
            # the row this records should read `ok`.
            State(
                source_id="hackernews",
                last_success_at=now - timedelta(seconds=20),
                last_content_change_at=now - timedelta(seconds=20),
            ),
            docs_in_window=600,
            now=now,
        ),
        assess(
            SOURCES["rss_tech"],
            State(source_id="rss_tech", last_success_at=now - timedelta(hours=5)),
            docs_in_window=0,
            now=now,
        ),
    ]


def test_records_one_row_per_source(spark):
    from signal_core.spark.jobs.health_snapshot import record

    now = utc_now()
    window_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    written = record(spark, [v.health for v in _verdicts(now)], window_start, table=TABLE)

    assert written == 2
    rows = {r.source_id: r for r in spark.table(TABLE).collect()}
    assert rows["hackernews"].status == "ok"
    assert rows["rss_tech"].status == "stale"
    assert rows["rss_tech"].gap_reason is not None


def test_rerunning_a_window_corrects_rather_than_duplicates(spark):
    """The monitoring DAG is re-runnable. A history table that doubles its rows on a
    re-run cannot answer "was the pipeline healthy last Tuesday?" — SPEC §11."""
    from signal_core.spark.jobs.health_snapshot import record

    now = utc_now()
    window_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    verdicts = _verdicts(now)

    record(spark, [v.health for v in verdicts], window_start, table=TABLE)
    # Second pass sees the source recovered.
    recovered = assess(
        SOURCES["rss_tech"],
        State(source_id="rss_tech", last_success_at=now - timedelta(minutes=1)),
        docs_in_window=6,
        now=now,
    )
    record(spark, [recovered.health], window_start, table=TABLE)

    rows = spark.table(TABLE).collect()
    assert len(rows) == 2
    assert {r.source_id: r.status for r in rows}["rss_tech"] == "ok"


def test_never_succeeded_stores_null_staleness_not_infinity(spark):
    """`inf` is honest in Python and poison in a double column anyone will later average."""
    from signal_core.spark.jobs.health_snapshot import record

    now = utc_now()
    verdict = assess(SOURCES["edgar"], State(source_id="edgar"), docs_in_window=0, now=now)

    record(spark, [verdict.health], now, table=TABLE)

    row = spark.table(TABLE).collect()[0]
    assert row.staleness_seconds is None
    assert row.status == "never_succeeded"
