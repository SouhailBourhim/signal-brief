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
SCORES_TABLE = "silver.hn_score_snapshots"

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
    for table in (BRONZE_TABLE, ARTICLES_TABLE, COMMENTS_TABLE, REJECTS_TABLE, SCORES_TABLE):
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


def test_external_id_is_carried_into_silver_articles(spark, staging):
    """4A.H. `article_id` is derived from content, so it changes when a headline is edited
    — exactly when a story is still developing. The source's own id is the stable key
    velocity needs to join a cluster to the score snapshots taken of it."""
    from signal_core.spark.jobs.normalize import normalize_window

    now = utc_now()
    _commit(spark, staging, [_hn_doc(1, "story.json", fetched_at=now)])
    normalize_window(spark, *_window(now))

    row = spark.table(ARTICLES_TABLE).collect()[0]
    assert row.external_id == "49350858", "the HN item id, as the source assigned it"


# --- normalize_hn_scores_window: a measurement, not a document ------------------------


def _scores_doc(index: int, name: str = "story.json", **kwargs) -> RawDocument:
    """Same fixture bytes as `_hn_doc`, a different `source_id` — which is exactly the
    real relationship between the two sources: one endpoint, two readings."""
    return _doc(
        index,
        source_id="hn_scores",
        payload=_fixture("hackernews", name),
        payload_format=PayloadFormat.JSON,
        **kwargs,
    )


def test_hn_scores_window_extracts_snapshots_and_no_articles(spark, staging):
    now = utc_now()
    _commit(spark, staging, [_scores_doc(1, fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_hn_scores_window, normalize_window

    scores_result = normalize_hn_scores_window(spark, *_window(now))
    articles_result = normalize_window(spark, *_window(now))

    assert scores_result.hn_scores_rows == 1
    assert scores_result.snapshots_committed == 1
    assert articles_result.articles_committed == 0, "a snapshot must never become an article"

    row = spark.table(SCORES_TABLE).collect()[0]
    assert row.item_id == "49350858"
    assert row.score == 1
    assert row.observed_at is not None


def test_the_articles_pass_does_not_count_hn_scores_rows(spark, staging):
    """`NON_ARTICLE_SOURCES`. `hn_scores` commits ~240 documents an hour and none of them
    is ever an article, so counting them in `bronze_rows` would show a rising bronze count
    against a flat article count forever — indistinguishable from a broken parser."""
    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _scores_doc(1, fetched_at=now),
            _doc(2, payload=_fixture("rss_tech", "feed.xml"), fetched_at=now),
        ],
    )

    from signal_core.spark.jobs.normalize import normalize_window

    result = normalize_window(spark, *_window(now))
    assert result.bronze_rows == 1, "the hn_scores row should not be counted here at all"
    assert result.articles_committed > 0


def test_the_same_story_snapshotted_twice_is_two_rows(spark, staging):
    """The whole point of the table, and the one place its MERGE key differs from its
    siblings'. Deduping on `item_id` would keep one score per story and delete the slope."""
    from signal_core.spark.jobs.normalize import normalize_hn_scores_window

    now = utc_now()
    later = now + timedelta(minutes=15)
    _commit(spark, staging, [_scores_doc(1, fetched_at=now)])
    _commit(spark, staging, [_scores_doc(2, fetched_at=later)])

    normalize_hn_scores_window(spark, *_window(now))
    normalize_hn_scores_window(spark, *_window(later))

    rows = spark.table(SCORES_TABLE).collect()
    assert len(rows) == 2, "two observations of one story are two rows"
    assert {r.item_id for r in rows} == {"49350858"}
    assert len({r.observed_at for r in rows}) == 2


def test_hn_scores_window_replay_commits_nothing_new(spark, staging):
    """SPEC §6.3: replay is deterministic. MERGE on `ingest_id`, which is unique per fetch."""
    now = utc_now()
    _commit(spark, staging, [_scores_doc(1, fetched_at=now)])
    from signal_core.spark.jobs.normalize import normalize_hn_scores_window

    first = normalize_hn_scores_window(spark, *_window(now))
    replay = normalize_hn_scores_window(spark, *_window(now))
    assert first.snapshots_committed == 1
    assert replay.snapshots_committed == 0


def test_a_comment_in_the_hn_scores_partition_yields_no_snapshot(spark, staging):
    """Defensive: the top-stories list should never contain a comment, and if HN changes
    what that list means, the parser warns and contributes nothing rather than recording
    a scoreless row."""
    now = utc_now()
    _commit(spark, staging, [_scores_doc(1, "comment.json", fetched_at=now)])

    from signal_core.spark.jobs.normalize import normalize_hn_scores_window

    result = normalize_hn_scores_window(spark, *_window(now))
    assert result.hn_scores_rows == 1
    assert result.snapshots_committed == 0


def test_article_id_stays_unique_across_reruns(spark, staging):
    """`silver.articles` MERGEs on `article_id`, so re-running a window must not duplicate.

    Sequential by construction, so it does not reproduce the concurrent case that actually
    put 132 duplicate ids in the deployed table — MERGE's NOT MATCHED clause compiles to an
    append, and Iceberg appends do not conflict. It pins the contract, and the table property
    `ensure_tables` now sets (serializable merge isolation) is what makes the concurrent
    writer fail instead of duplicate.
    """
    from signal_core.spark.jobs.normalize import normalize_window

    now = utc_now()
    payload = _fixture("rss_tech", "feed.xml")
    _commit(spark, staging, [_doc(1, source_id="rss_tech", payload=payload, fetched_at=now)])
    normalize_window(spark, *_window(now))
    normalize_window(spark, *_window(now))

    rows = spark.table(ARTICLES_TABLE).count()
    distinct = spark.table(ARTICLES_TABLE).select("article_id").distinct().count()
    assert rows == distinct, f"{rows - distinct} duplicate article_id rows after a re-run"


def test_repair_collapses_duplicate_article_ids(spark, staging):
    """`spark/jobs/repair.py`. Duplicates cannot be produced through the normal path — a
    sequential re-run MERGEs cleanly — so this writes them the way the incident did: an
    append that bypasses the MERGE entirely, which is exactly what a second concurrent
    writer's `WHEN NOT MATCHED` clause degenerates into."""
    from signal_core.spark.jobs.normalize import normalize_window
    from signal_core.spark.jobs.repair import find_duplicates, repair_duplicates

    now = utc_now()
    payload = _fixture("rss_tech", "feed.xml")
    _commit(spark, staging, [_doc(1, source_id="rss_tech", payload=payload, fetched_at=now)])
    normalize_window(spark, *_window(now))

    clean_rows = spark.table(ARTICLES_TABLE).count()
    spark.table(ARTICLES_TABLE).limit(3).writeTo(ARTICLES_TABLE).append()
    assert spark.table(ARTICLES_TABLE).count() == clean_rows + 3
    assert find_duplicates(spark, ARTICLES_TABLE).count() == 3

    planned = repair_duplicates(spark, ARTICLES_TABLE, dry_run=True)
    assert planned.dry_run and planned.duplicate_ids == 3
    assert planned.rows_removed == 3
    # A dry run must not touch the table.
    assert spark.table(ARTICLES_TABLE).count() == clean_rows + 3

    done = repair_duplicates(spark, ARTICLES_TABLE, dry_run=False)
    assert not done.dry_run
    assert done.rows_after == clean_rows
    assert find_duplicates(spark, ARTICLES_TABLE).count() == 0
    # Partition-mates of the repaired rows must survive the overwrite.
    assert spark.table(ARTICLES_TABLE).select("article_id").distinct().count() == clean_rows


def test_repair_is_a_no_op_on_a_clean_table(spark, staging):
    from signal_core.spark.jobs.normalize import normalize_window
    from signal_core.spark.jobs.repair import repair_duplicates

    now = utc_now()
    _commit(spark, staging, [_doc(1, payload=_fixture("rss_tech", "feed.xml"), fetched_at=now)])
    normalize_window(spark, *_window(now))
    before = spark.table(ARTICLES_TABLE).count()

    result = repair_duplicates(spark, ARTICLES_TABLE, dry_run=False)

    assert result.duplicate_ids == 0
    assert result.rows_removed == 0
    assert spark.table(ARTICLES_TABLE).count() == before
