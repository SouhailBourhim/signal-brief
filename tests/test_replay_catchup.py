"""SPEC §12's Phase 1 acceptance test: stop ingestion for a day, restart.

    Replay reprocesses the stored interval with no duplicates and no gaps; catch-up
    recovers what each source's backfill horizon allows and records `gap_reason` for
    the rest.

Two halves, deliberately asserted separately because they promise different things
(SPEC §6.3). Replay is mechanical and total: the bytes are in bronze, reprocessing them
is arithmetic. Catch-up is bounded by what a source will still serve, and the honest
result for an RSS feed after a day of downtime is "most of that is gone" — the test
exists to prove the pipeline *says* so.

The replay half needs Spark and is marked accordingly; the catch-up half is pure
arithmetic over horizons and runs everywhere.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.ops.health import assess_source
from signal_core.ops.recovery import plan_catch_up
from signal_core.staging import write_staging
from signal_core.timeutil import utc_now

TABLE = "bronze.raw_documents"
OUTAGE_HOURS = 24


def _day_of_documents(source_id: str, *, hours: int = 6, per_hour: int = 3):
    """A day's ingestion, one staged batch per hour — the shape a scheduled poller
    actually produces, and the thing an interval replay has to reproduce exactly."""
    start = utc_now() - timedelta(hours=hours)
    documents = []
    for hour in range(hours):
        for index in range(per_hour):
            fetched_at = start + timedelta(hours=hour, minutes=index)
            payload = f"<item>{source_id}-{hour}-{index}</item>".encode()
            documents.append(
                RawDocument(
                    ingest_id=f"{source_id}-{hour:02d}{index:02d}",
                    source_id=source_id,
                    fetched_at=fetched_at,
                    source_url=f"https://example.test/{hour}/{index}",
                    http_status=200,
                    outcome=FetchOutcome.OK,
                    etag=None,
                    last_modified=None,
                    content_hash=f"{hour:032d}{index:032d}",
                    payload=payload,
                    payload_format=PayloadFormat.XML,
                    latency_ms=10,
                    byte_count=len(payload),
                )
            )
    return documents


# --------------------------------------------------------------------------------------
# Replay — from stored bytes. Always possible, always exact.
# --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    session = build_iceberg_session(
        "signal-replay-test", warehouse=tmp_path_factory.mktemp("warehouse"), catalog="test"
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_replay_of_a_days_interval_has_no_duplicates_and_no_gaps(spark, tmp_path):
    from signal_core.spark.jobs.commit_bronze import commit

    staging = tmp_path / "staging"
    documents = _day_of_documents("rss_tech")
    for hour_batch in _by_hour(documents):
        write_staging(hour_batch, staging)

    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    first = commit(spark, staging, table=TABLE)

    # Ingestion stops. Nothing new is staged. The operator re-runs the interval.
    replay = commit(spark, staging, table=TABLE)

    assert first.committed_rows == len(documents)
    assert replay.committed_rows == 0, "replay must not duplicate what bronze already has"
    assert replay.table_rows == len(documents)

    # No gaps: every document that was staged is in bronze, by id, not by count — a
    # count match can hide one document lost and another duplicated.
    committed_ids = {r.ingest_id for r in spark.table(TABLE).select("ingest_id").collect()}
    assert committed_ids == {d.ingest_id for d in documents}


def _by_hour(documents):
    batches: dict[int, list] = {}
    for doc in documents:
        batches.setdefault(doc.fetched_at.hour, []).append(doc)
    return list(batches.values())


# --------------------------------------------------------------------------------------
# Catch-up — from the source. Bounded by the horizon, and honest about the remainder.
# --------------------------------------------------------------------------------------


@pytest.fixture
def outage():
    """A 24-hour outage that ended two hours ago — the realistic case, because nobody
    notices at the instant the pipeline comes back."""
    now = utc_now()
    end = now - timedelta(hours=2)
    return now, end - timedelta(hours=OUTAGE_HOURS), end


def test_complete_horizon_recovers_the_whole_outage(outage):
    """Hacker News item ids are sequential and dense: the watermark walk that resumes
    after downtime is the same walk it does normally, just longer. SPEC §3."""
    now, start, end = outage
    plan = plan_catch_up(SOURCES["hackernews"], start, end, now=now)

    assert plan.is_complete
    assert plan.gap_reason is None
    assert plan.recoverable_from == start
    assert plan.recoverable_until == end


def test_day_horizon_recovers_the_recent_part_and_reports_the_rest(outage):
    """EDGAR's current-filings feed reaches back about a day, so a 24-hour outage
    noticed two hours late loses its first two hours. SPEC §3."""
    now, start, end = outage
    plan = plan_catch_up(SOURCES["edgar"], start, end, now=now)

    assert not plan.is_complete
    assert plan.has_work
    assert plan.recoverable_from == now - timedelta(days=1)
    assert plan.gap_start == start
    assert plan.gap_seconds == pytest.approx(2 * 3600, abs=1)
    assert "day horizon" in plan.gap_reason


def test_window_horizon_recovers_nothing_and_says_so(outage):
    """The one that matters. An RSS feed holds its current window and nothing else: a
    day of downtime is a day of permanent loss, and pretending otherwise is the failure
    SPEC §6.3 was written against."""
    now, start, end = outage
    plan = plan_catch_up(SOURCES["rss_tech"], start, end, now=now)

    assert not plan.is_complete
    assert not plan.has_work
    assert plan.gap_start == start and plan.gap_end == end
    assert plan.gap_seconds == pytest.approx(OUTAGE_HOURS * 3600, abs=1)
    assert "rotated out" in plan.gap_reason


def test_gap_reason_reaches_the_health_footer(outage):
    """A gap nobody can see is the same as no gap. SPEC §6.3 puts it in
    `ops.source_health`, which is what the brief's footer renders."""
    now, start, end = outage
    plan = plan_catch_up(SOURCES["rss_tech"], start, end, now=now)

    health = assess_source(
        SOURCES["rss_tech"],
        docs_ingested=12,
        last_success_at=now - timedelta(minutes=5),
        now=now,
        gap_reason=plan.gap_reason,
    )

    assert health.status == "gapped"
    assert health.gap_reason == plan.gap_reason


def test_a_source_that_was_never_down_plans_no_work():
    now = utc_now()
    plan = plan_catch_up(SOURCES["rss_tech"], now, now, now=now)

    assert plan.is_complete
    assert not plan.has_work
    assert plan.gap_seconds == 0.0
