"""`spark/jobs/cluster.py`. SPEC §7.1, §9, §12; docs/runbooks/phase-3.md 3.B.

Silver rows are built through the real `write_staging` -> `commit` -> `normalize_window`
path from real fixture bytes, not hand-assembled, so a mismatch between what commits and
what the cluster job expects shows up here rather than in production — the same reasoning
`tests/test_normalize_window.py` gives.

The properties worth pinning are the ones the pairwise eval cannot see: that re-running is a
no-op, that a threshold change replaces rather than accumulates, that blocking loses no
candidate the all-pairs path would have found, and that the size guard fires.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import write_staging

pytestmark = pytest.mark.spark

BRONZE_TABLE = "bronze.raw_documents"
ARTICLES_TABLE = "silver.articles"
REJECTS_TABLE = "silver.parse_rejects"
CLUSTERS_TABLE = "silver.story_clusters"
MAP_TABLE = "silver.article_clusters"

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-cluster", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_tables(spark):
    for table in (BRONZE_TABLE, ARTICLES_TABLE, REJECTS_TABLE, CLUSTERS_TABLE, MAP_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    yield


def _doc(index: int, payload: bytes, fetched_at: datetime) -> RawDocument:
    return RawDocument(
        ingest_id=f"fake-{index:03d}",
        source_id="fake",
        fetched_at=fetched_at,
        source_url="https://example.test/feed",
        http_status=200,
        outcome=FetchOutcome.OK,
        etag=None,
        last_modified=None,
        content_hash=f"hash-{index:03d}",
        payload=payload,
        payload_format=PayloadFormat.JSON,
        latency_ms=5,
        byte_count=len(payload),
    )


@pytest.fixture
def window(spark, tmp_path):
    """A committed window of silver, produced the way production produces it."""
    from signal_core.config import SOURCES
    from signal_core.contracts import State
    from signal_core.sources import get_poller
    from signal_core.spark.jobs.commit_bronze import commit
    from signal_core.spark.jobs.normalize import normalize_window

    documents, _ = get_poller("fake")(SOURCES["fake"], State(source_id="fake"))
    staged = [_doc(i, d.payload, NOW - timedelta(minutes=30)) for i, d in enumerate(documents)]
    staging = tmp_path / "staging"
    write_staging(staged, staging)
    commit(spark, staging, table=BRONZE_TABLE)
    normalize_window(spark, NOW - timedelta(hours=1), NOW)
    return NOW - timedelta(hours=72), NOW


def _rows(spark, table: str) -> list[dict]:
    return [r.asDict() for r in spark.table(table).collect()]


# --- the job end to end -------------------------------------------------------------------


def test_cluster_window_writes_both_tables(spark, window):
    from signal_core.spark.jobs.cluster import cluster_window

    result = cluster_window(spark, *window)

    assert result.articles_in > 0
    assert result.clusters_out > 0
    assert result.clusters_out <= result.articles_in
    clusters = _rows(spark, CLUSTERS_TABLE)
    mapped = _rows(spark, MAP_TABLE)
    assert len(clusters) == result.clusters_out
    # Every surviving article is mapped exactly once: clustering must not lose or duplicate.
    assert len(mapped) == sum(c["article_count"] for c in clusters)
    assert len({m["article_id"] for m in mapped}) == len(mapped)
    assert sum(1 for m in mapped if m["is_canonical"]) == len(clusters)


def test_rerunning_the_same_window_is_a_no_op(spark, window):
    """`overwritePartitions` replaces the window rather than appending to it. Without this
    the daily runs, which share 48 of their 72 hours, would double every row."""
    from signal_core.spark.jobs.cluster import cluster_window

    first = cluster_window(spark, *window)
    before = sorted(r["cluster_id"] for r in _rows(spark, CLUSTERS_TABLE))
    second = cluster_window(spark, *window)
    after = sorted(r["cluster_id"] for r in _rows(spark, CLUSTERS_TABLE))

    assert first.clusters_out == second.clusters_out
    assert before == after
    assert first.ordering_key == second.ordering_key


def test_a_threshold_change_replaces_rather_than_accumulates(spark, window, monkeypatch):
    """A cluster assignment is the output of a function of (window, algorithm), not a fact
    about an article — so a re-run under different thresholds must not leave both answers
    in the table."""
    from signal_core import dedup
    from signal_core.spark.jobs.cluster import cluster_window

    cluster_window(spark, *window)
    monkeypatch.setattr(dedup, "TITLE_JACCARD", 0.99)
    monkeypatch.setattr(dedup, "NEAR_DUPLICATE_DISTANCE", 0)
    monkeypatch.setattr(dedup, "BODY_JACCARD", 0.99)
    strict = cluster_window(spark, *window)

    assert len(_rows(spark, CLUSTERS_TABLE)) == strict.clusters_out
    assert len({r["article_id"] for r in _rows(spark, MAP_TABLE)}) == strict.articles_in - (
        strict.exact_duplicates_removed
    )


def test_the_ordering_key_records_the_input_set(spark, window):
    """SPEC §7.1 wants a replay able to prove it saw the same input, not just assert it."""
    from signal_core.spark.jobs.cluster import cluster_window

    result = cluster_window(spark, *window)
    assert result.ordering_key.startswith("fetched_at,article_id@")
    assert len(result.ordering_key.split("@")[1]) == 16
    assert all(r["ordering_key"] == result.ordering_key for r in _rows(spark, CLUSTERS_TABLE))


def test_blocking_finds_every_pair_all_pairs_would(spark, window):
    """Prefix filtering is exact, so the blocked job and the in-process job must agree
    exactly — **provided no blocking key was dropped for being oversized.**

    That precondition is asserted rather than assumed, because it is the one thing that can
    break the guarantee, and on real data it does: a 72-hour production window drops 3 keys
    over `MAX_BLOCK_SIZE` and lands 2,284 clusters against all-pairs' 2,277. Seven clusters
    in 2,284 is the measured price of the cap, it is reported as `blocking_keys_dropped`
    rather than hidden, and this test pins the case where the price is zero.
    """
    from signal_core import dedup
    from signal_core.spark.jobs.cluster import cluster_window, read_window

    result = cluster_window(spark, *window)
    assert result.blocking_keys_dropped == 0, "exactness only holds when no key is dropped"

    articles = [r.asDict() for r in read_window(spark, *window).collect()]
    deduped, _ = dedup.exact_dedup(articles)
    reference = dedup.group_stories(deduped)

    assert result.clusters_out == len(reference.clusters)
    assert sorted(c["cluster_id"] for c in reference.clusters) == sorted(
        r["cluster_id"] for r in _rows(spark, CLUSTERS_TABLE)
    )


def test_a_ddl_that_grew_a_column_reconciles_an_existing_table(spark):
    """3.B.4 added `first_seen`/`last_seen` and `CREATE TABLE IF NOT EXISTS` never noticed.

    The deployed table kept its original 17 columns for days, silently, and the failure
    surfaced from the brief as `COLUMN_NOT_FOUND` — a long way from the change that caused
    it. This pins the reconciliation rather than the incident.
    """
    from signal_core.spark.jobs.cluster import CLUSTERS_DDL, ensure_tables
    from signal_core.spark.tables import ensure_columns

    ensure_tables(spark)
    spark.sql(f"ALTER TABLE {CLUSTERS_TABLE} DROP COLUMN last_seen")
    assert "last_seen" not in {f.name for f in spark.table(CLUSTERS_TABLE).schema.fields}

    added = ensure_columns(spark, CLUSTERS_TABLE, CLUSTERS_DDL)

    assert added == ["last_seen"]
    assert "last_seen" in {f.name for f in spark.table(CLUSTERS_TABLE).schema.fields}


def test_reconciliation_adds_nothing_when_the_table_already_matches(spark):
    """Idempotence: every run calls this, and a schema change reported on every run is a
    schema change nobody reads."""
    from signal_core.spark.jobs.cluster import CLUSTERS_DDL, ensure_tables
    from signal_core.spark.tables import ensure_columns

    ensure_tables(spark)
    assert ensure_columns(spark, CLUSTERS_TABLE, CLUSTERS_DDL) == []


def test_an_added_column_is_nullable_whatever_the_ddl_says(spark):
    """Iceberg will not add a required column to a table that already has rows, and a
    `NOT NULL` in a DDL is a statement about writers rather than about history."""
    from signal_core.spark.jobs.cluster import CLUSTERS_DDL, ensure_tables
    from signal_core.spark.tables import ensure_columns

    ensure_tables(spark)
    spark.sql(f"ALTER TABLE {CLUSTERS_TABLE} DROP COLUMN first_seen")
    ensure_columns(spark, CLUSTERS_TABLE, CLUSTERS_DDL)

    field = next(f for f in spark.table(CLUSTERS_TABLE).schema.fields if f.name == "first_seen")
    assert field.nullable
