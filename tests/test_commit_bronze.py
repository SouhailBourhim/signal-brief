"""Staging -> Iceberg `bronze.raw_documents`. SPEC §6.4, §12.

Marked `spark`: needs a JVM, and the first run resolves the Iceberg runtime jar from
Maven. The warehouse is a temp directory with a Hadoop catalog rather than Glue, which is
the same SQL against the same table format — the catalog is the only difference, and it
is the part Terraform owns rather than this code.
"""

from __future__ import annotations

import pytest

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import write_staging
from signal_core.timeutil import utc_now

pytestmark = pytest.mark.spark

TABLE = "bronze.raw_documents"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture
def staging(tmp_path):
    return tmp_path / "staging"


@pytest.fixture(autouse=True)
def clean_table(spark):
    """Each test starts from an empty table — the module-scoped session is shared, the
    data is not."""
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    yield


def _doc(index: int, *, payload: bytes = b"<item/>", outcome=FetchOutcome.OK) -> RawDocument:
    return RawDocument(
        ingest_id=f"rss_tech-{index:04d}",
        source_id="rss_tech",
        fetched_at=utc_now(),
        source_url=f"https://example.test/{index}",
        http_status=200 if outcome is FetchOutcome.OK else 503,
        outcome=outcome,
        etag='W/"abc"',
        last_modified=None,
        content_hash=f"{index:064d}",
        payload=payload,
        payload_format=PayloadFormat.XML,
        latency_ms=12,
        byte_count=len(payload),
    )


def test_commit_inserts_staged_documents(spark, staging):
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1), _doc(2)], staging)
    result = commit(spark, staging, table=TABLE)

    assert result.staged_rows == 2
    assert result.committed_rows == 2
    assert result.table_rows == 2


def test_replay_of_a_committed_interval_adds_nothing(spark, staging):
    """SPEC §12's Phase 1 acceptance test, in miniature: reprocessing an interval already
    in bronze produces no duplicates, however many times it runs."""
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1), _doc(2), _doc(3)], staging)
    first = commit(spark, staging, table=TABLE)
    replay = commit(spark, staging, table=TABLE)

    assert first.committed_rows == 3
    assert replay.committed_rows == 0
    assert replay.duplicate_rows == replay.staged_rows == 3
    assert replay.table_rows == 3


def test_overlapping_catch_up_window_does_not_duplicate(spark, staging):
    """Catch-up re-fetches a window wider than the gap, so the same document arrives in
    two different staged objects. The MERGE, not luck, is what collapses them."""
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1), _doc(2)], staging)
    write_staging([_doc(2), _doc(3)], staging)  # overlaps by one

    result = commit(spark, staging, table=TABLE)

    assert result.staged_rows == 3  # deduplicated within the batch
    assert result.table_rows == 3


def test_payload_bytes_survive_the_round_trip(spark, staging):
    """The bytes in bronze are the bytes fetched. Everything downstream is recomputable
    from them (SPEC §6.2), so an encoding slip here is unrecoverable."""
    from signal_core.spark.jobs.commit_bronze import commit

    payload = b"\xff\xfe caf\xe9 \x00 <item/>"
    write_staging([_doc(1, payload=payload)], staging)
    commit(spark, staging, table=TABLE)

    row = spark.table(TABLE).select("payload", "byte_count").collect()[0]
    assert bytes(row.payload) == payload
    assert row.byte_count == len(payload)


def test_failed_fetches_are_committed_not_dropped(spark, staging):
    """SPEC §6.2: quarantined with a reason. A failure that vanishes on the way into
    bronze is a gap nothing downstream can see."""
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1), _doc(2, outcome=FetchOutcome.ERROR)], staging)
    commit(spark, staging, table=TABLE)

    rows = spark.sql(f"SELECT outcome, count(*) n FROM {TABLE} GROUP BY outcome").collect()
    assert {r.outcome: r.n for r in rows} == {
        FetchOutcome.OK.value: 1,
        FetchOutcome.ERROR.value: 1,
    }


def test_table_is_partitioned_by_source_and_ingest_date(spark, staging):
    """SPEC §6.4. Asserted because the partitioning is the thing that keeps a one-source
    backfill from rewriting every other source's data."""
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1)], staging)
    commit(spark, staging, table=TABLE)

    fields = [r.partition for r in spark.sql(f"SELECT partition FROM {TABLE}.partitions").collect()]
    assert fields[0].source_id == "rss_tech"
    assert fields[0].ingest_date is not None


def test_source_filter_reads_only_that_source(spark, staging):
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging([_doc(1)], staging)
    write_staging([_doc(9).model_copy(update={"source_id": "hackernews"})], staging)

    commit(spark, staging, table=TABLE, source_id="rss_tech")

    assert [r.source_id for r in spark.table(TABLE).select("source_id").distinct().collect()] == [
        "rss_tech"
    ]
