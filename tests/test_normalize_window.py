"""`normalize_window` / `normalize_hn_comments_window`: bronze.raw_documents (Iceberg)
-> silver.articles / silver.hn_comments / silver.parse_rejects. docs/runbooks/phase-2.md
2.C.

Marked `spark`, same shape as `test_commit_bronze.py`: a Hadoop-catalog warehouse in a
temp directory stands in for Glue, and every test starts from real bronze rows produced
through `staging.write_staging` + `commit_bronze.commit` — the same path production
uses — rather than hand-assembled table rows, so a mismatch between what commits and
what normalize expects shows up here.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import write_staging
from signal_core.timeutil import utc_now

pytestmark = pytest.mark.spark

BRONZE_TABLE = "bronze.raw_documents"
ARTICLES_TABLE = "silver.articles"
COMMENTS_TABLE = "silver.hn_comments"
REJECTS_TABLE = "silver.parse_rejects"

FIXTURES = Path(__file__).parent / "fixtures" / "bronze"


def _fixture(source: str, name: str) -> bytes:
    return (FIXTURES / source / name).read_bytes()


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-normalize", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture
def staging(tmp_path):
    return tmp_path / "staging"


@pytest.fixture(autouse=True)
def clean_tables(spark):
    """Each test starts from empty tables — the module-scoped session is shared, the
    data is not."""
    for table in (BRONZE_TABLE, ARTICLES_TABLE, COMMENTS_TABLE, REJECTS_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    yield


def _doc(
    index: int,
    *,
    source_id: str = "rss_tech",
    payload: bytes = b"<item/>",
    payload_format: PayloadFormat = PayloadFormat.XML,
    outcome: FetchOutcome = FetchOutcome.OK,
    fetched_at: datetime | None = None,
) -> RawDocument:
    return RawDocument(
        ingest_id=f"{source_id}-{index:04d}",
        source_id=source_id,
        fetched_at=fetched_at or utc_now(),
        source_url=f"https://example.test/{source_id}/{index}",
        http_status=200 if outcome is FetchOutcome.OK else 503,
        outcome=outcome,
        etag=None,
        last_modified=None,
        content_hash=f"{index:064d}",
        payload=payload,
        payload_format=payload_format,
        latency_ms=12,
        byte_count=len(payload),
    )


def _commit(spark, staging, docs) -> None:
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging(docs, staging)
    commit(spark, staging, table=BRONZE_TABLE)


def _window(fetched_at: datetime) -> tuple[datetime, datetime]:
    return fetched_at - timedelta(minutes=1), fetched_at + timedelta(minutes=1)


# --- normalize_window: real feeds, real MERGE -----------------------------------------


def test_normalize_window_commits_articles_from_a_real_feed(spark, staging):
    from signal_core.parse import get_parser
    from signal_core.spark.jobs.normalize import normalize_window

    now = utc_now()
    payload = _fixture("rss_tech", "feed.xml")
    expected_items = len(get_parser("rss_tech")(payload).items)
    _commit(spark, staging, [_doc(1, source_id="rss_tech", payload=payload, fetched_at=now)])

    result = normalize_window(spark, *_window(now))

    assert result.bronze_rows == 1
    assert result.skipped_rows == 0
    assert result.articles_committed == expected_items
    assert result.articles_table_rows == expected_items
    assert result.rejects_committed == 0

    row = (
        spark.table(ARTICLES_TABLE)
        .where(
            "title = 'Cursor capitalizes on GitHub frustration, launches rival hosting platform'"
        )
        .select("published_at", "event_date", "parse_error")
        .collect()[0]
    )
    assert row.parse_error is None
    # ADR-0007: event_date is published_at when it's known.
    assert row.event_date == row.published_at


def test_normalize_window_replay_commits_nothing_new(spark, staging):
    """SPEC §6.3's replay guarantee, on the silver side: reprocessing an interval
    already in bronze produces no duplicates."""
    from signal_core.spark.jobs.normalize import normalize_window

    now = utc_now()
    _commit(spark, staging, [_doc(1, payload=_fixture("rss_ars", "feed.xml"), fetched_at=now)])

    first = normalize_window(spark, *_window(now))
    replay = normalize_window(spark, *_window(now))

    assert first.articles_committed > 0
    assert replay.articles_committed == 0
    assert replay.articles_table_rows == first.articles_table_rows


def test_normalize_window_skips_error_and_empty_rows_but_counts_them(spark, staging):
    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(1, payload=_fixture("rss_verge", "feed.xml"), fetched_at=now),
            _doc(2, payload=b"boom", outcome=FetchOutcome.ERROR, fetched_at=now),
            _doc(3, payload=b"", outcome=FetchOutcome.EMPTY, fetched_at=now),
        ],
    )
    from signal_core.parse import get_parser
    from signal_core.spark.jobs.normalize import normalize_window

    expected_items = len(get_parser("rss_verge")(_fixture("rss_verge", "feed.xml")).items)
    result = normalize_window(spark, *_window(now))

    assert result.bronze_rows == 1
    assert result.skipped_rows == 2
    assert result.articles_committed == expected_items


def test_normalize_window_row_level_failure_lands_in_parse_rejects_not_articles(spark, staging):
    """A totally malformed feed (not the EDGAR-encoding-lie case, an outright bad one)
    quarantines as one `silver.parse_rejects` row — SPEC §6.2 — never `silver.articles`."""
    now = utc_now()
    _commit(spark, staging, [_doc(1, payload=b"not xml at all", fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_window

    result = normalize_window(spark, *_window(now))

    assert result.articles_committed == 0
    assert result.articles_table_rows == 0
    assert result.rejects_committed == 1
    reject = spark.table(REJECTS_TABLE).collect()[0]
    assert reject.ingest_id == "rss_tech-0001"
    assert reject.source_id == "rss_tech"
    assert reject.parse_error is not None
    assert reject.rejected_at is not None
    assert "payload" not in spark.table(REJECTS_TABLE).columns


def test_normalize_window_edgar_iso_8859_1_bytes_commit_cleanly(spark, staging):
    """The real cross-cutting case: EDGAR's declared-Latin-1 bytes must survive staging's
    base64 round trip, bronze's MERGE, and the Iceberg write, and still parse."""
    now = utc_now()
    payload = _fixture("edgar", "feed.xml")
    _commit(spark, staging, [_doc(1, source_id="edgar", payload=payload, fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_window

    result = normalize_window(spark, *_window(now))
    assert result.articles_committed > 1
    assert result.rejects_committed == 0


def test_normalize_window_ignores_rows_outside_the_window(spark, staging):
    now = utc_now()
    old = now - timedelta(hours=6)
    _commit(
        spark,
        staging,
        [
            _doc(1, payload=_fixture("rss_tech", "feed.xml"), fetched_at=now),
            _doc(2, payload=_fixture("rss_ars", "feed.xml"), fetched_at=old),
        ],
    )
    from signal_core.spark.jobs.normalize import normalize_window

    result = normalize_window(spark, *_window(now))
    assert result.bronze_rows == 1  # the `old` row's ingest_date/fetched_at fall outside


def test_articles_table_is_partitioned_by_event_date(spark, staging):
    now = utc_now()
    _commit(spark, staging, [_doc(1, payload=_fixture("rss_tech", "feed.xml"), fetched_at=now)])
    from signal_core.spark.jobs.normalize import normalize_window

    normalize_window(spark, *_window(now))
    partitions = spark.sql(f"SELECT partition FROM {ARTICLES_TABLE}.partitions").collect()
    assert partitions, "MERGE must have produced at least one partition"
    assert hasattr(partitions[0].partition, "event_date_day")


# --- normalize_hn_comments_window: comments only, story_id resolution -----------------


def _hn_doc(index: int, name: str, **kwargs) -> RawDocument:
    return _doc(
        index,
        source_id="hackernews",
        payload=_fixture("hackernews", name),
        payload_format=PayloadFormat.JSON,
        **kwargs,
    )


def test_hn_comments_window_extracts_comments_not_articles(spark, staging):
    now = utc_now()
    _commit(spark, staging, [_hn_doc(1, "comment.json", fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_hn_comments_window, normalize_window

    comments_result = normalize_hn_comments_window(spark, *_window(now))
    articles_result = normalize_window(spark, *_window(now))

    assert comments_result.hackernews_rows == 1
    assert comments_result.comments_committed == 1
    assert articles_result.articles_committed == 0, "a comment must never become an article"

    row = spark.table(COMMENTS_TABLE).collect()[0]
    assert row.item_id == "49350860"
    assert row.parent_id == "49348545"


def test_hn_comments_window_story_stays_out_of_hn_comments(spark, staging):
    """`type in (story, job)` goes to `silver.articles`, never `silver.hn_comments`."""
    now = utc_now()
    _commit(spark, staging, [_hn_doc(1, "story.json", fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_hn_comments_window, normalize_window

    normalize_hn_comments_window(spark, *_window(now))
    articles_result = normalize_window(spark, *_window(now))

    assert spark.table(COMMENTS_TABLE).count() == 0
    assert articles_result.articles_committed == 1


def test_hn_comments_window_replay_commits_nothing_new(spark, staging):
    now = utc_now()
    _commit(spark, staging, [_hn_doc(1, "comment.json", fetched_at=now)])
    from signal_core.spark.jobs.normalize import normalize_hn_comments_window

    first = normalize_hn_comments_window(spark, *_window(now))
    replay = normalize_hn_comments_window(spark, *_window(now))
    assert first.comments_committed == 1
    assert replay.comments_committed == 0


def test_hn_comments_window_resolves_story_id_within_one_batch(spark, staging):
    """A synthetic three-level chain: story 100 <- comment A(id=200) <- comment B(id=300),
    all fetched in the same window. `story_id` for both A and B must resolve to 100."""
    now = utc_now()
    a = b'{"by":"a","id":200,"parent":100,"text":"top-level","time":1787000000,"type":"comment"}'
    b = b'{"by":"b","id":300,"parent":200,"text":"nested","time":1787000010,"type":"comment"}'
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                source_id="hackernews",
                payload=a,
                payload_format=PayloadFormat.JSON,
                fetched_at=now,
            ),
            _doc(
                2,
                source_id="hackernews",
                payload=b,
                payload_format=PayloadFormat.JSON,
                fetched_at=now,
            ),
        ],
    )
    from signal_core.spark.jobs.normalize import normalize_hn_comments_window

    normalize_hn_comments_window(spark, *_window(now))

    rows = {r.item_id: r.story_id for r in spark.table(COMMENTS_TABLE).collect()}
    assert rows["200"] == "100"
    assert rows["300"] == "100", "must walk through comment 200, not stop at its parent"


def test_hn_comments_window_resolves_story_id_across_committed_batches(spark, staging):
    """The chain's upper half lands in one run, the lower half in the next. Resolution
    must consult already-committed `silver.hn_comments`, not just the new batch."""
    now = utc_now()
    a = b'{"by":"a","id":200,"parent":100,"text":"top-level","time":1787000000,"type":"comment"}'
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                source_id="hackernews",
                payload=a,
                payload_format=PayloadFormat.JSON,
                fetched_at=now,
            )
        ],
    )
    from signal_core.spark.jobs.normalize import normalize_hn_comments_window

    normalize_hn_comments_window(spark, *_window(now))

    later = now + timedelta(hours=2)
    b = b'{"by":"b","id":300,"parent":200,"text":"nested reply","time":1787007200,"type":"comment"}'
    _commit(
        spark,
        staging,
        [
            _doc(
                2,
                source_id="hackernews",
                payload=b,
                payload_format=PayloadFormat.JSON,
                fetched_at=later,
            )
        ],
    )
    normalize_hn_comments_window(spark, *_window(later))

    row = spark.table(COMMENTS_TABLE).where("item_id = '300'").collect()[0]
    assert row.story_id == "100"


def test_hn_comments_window_unresolvable_ancestor_keeps_best_known_id(spark, staging):
    """A comment whose parent was never ingested: `story_id` falls back to the nearest
    known ancestor (its immediate parent) rather than raising or guessing further."""
    now = utc_now()
    orphan = (
        b'{"by":"o","id":900,"parent":800,"text":"reply to an ancestor we never fetched",'
        b'"time":1787000000,"type":"comment"}'
    )
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                source_id="hackernews",
                payload=orphan,
                payload_format=PayloadFormat.JSON,
                fetched_at=now,
            )
        ],
    )
    from signal_core.spark.jobs.normalize import normalize_hn_comments_window

    normalize_hn_comments_window(spark, *_window(now))
    row = spark.table(COMMENTS_TABLE).collect()[0]
    assert row.story_id == "800"
