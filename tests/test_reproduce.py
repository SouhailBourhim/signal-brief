"""The reproducibility harness. SPEC §12's 4B acceptance; §18; 4B.K.

Split deliberately: the reporting semantics need no Spark and are where the over-claiming
risk lives, so they are plain unit tests. The stage checks that need a warehouse are marked
`spark` and run a real replay against real Iceberg tables.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signal_core.ops.reproduce import (
    CLUSTER_AGREEMENT_TOLERANCE,
    ReproducibilityReport,
    StageReport,
    _co_members,
    _shadow,
)

SINCE = datetime(2026, 7, 23, tzinfo=UTC)
UNTIL = datetime(2026, 8, 22, tzinfo=UTC)


# --- what each claim means ----------------------------------------------------------------


def test_an_identical_claim_fails_on_a_single_mismatch():
    """ "Identical" has to mean identical. SPEC §18 names over-claiming reproducibility as a
    known failure mode, and a stage that tolerates "almost" while calling itself identical is
    that failure with extra steps."""
    assert StageReport("normalize", "identical", compared=1000, matched=1000).passed
    assert not StageReport("normalize", "identical", compared=1000, matched=999).passed


def test_a_tolerance_claim_passes_at_the_stated_bound_and_fails_below():
    """Clustering is order-dependent by nature, which is why `cluster.py` records an ordering
    key at all. The claim is a stated agreement rate, and the bound is checked rather than
    described."""
    at_bound = StageReport(
        "clustering", "tolerance", compared=100, matched=95, tolerance=CLUSTER_AGREEMENT_TOLERANCE
    )
    below = StageReport(
        "clustering", "tolerance", compared=100, matched=94, tolerance=CLUSTER_AGREEMENT_TOLERANCE
    )
    assert at_bound.passed
    assert not below.passed


def test_the_cache_stage_publishes_a_rate_and_never_gates_on_one():
    """SPEC §12 asks for "a published hit rate", not a floor. Inventing a floor would be a
    claim the spec did not make — §18's over-claiming failure in miniature — and a cold cache
    on a fresh clone would then fail an acceptance test about determinism."""
    cold = StageReport("enrichment", "cache", compared=40, matched=0)
    assert cold.passed
    assert cold.agreement == 0.0


def test_a_report_fails_if_any_stage_does():
    report = ReproducibilityReport(
        since=SINCE,
        until=UNTIL,
        stages=[
            StageReport("bronze bytes", "identical", compared=10, matched=10),
            StageReport("normalize", "identical", compared=10, matched=9),
        ],
    )
    assert not report.passed


def test_each_claim_is_named_in_the_rendered_report():
    """A reader has to be able to tell which stage claims what. One boolean called
    `reproducible` over five different guarantees is the thing this module exists not to be."""
    report = ReproducibilityReport(
        since=SINCE,
        until=UNTIL,
        stages=[
            StageReport("bronze bytes", "identical", compared=10, matched=10),
            StageReport("clustering", "tolerance", compared=10, matched=10, tolerance=0.95),
            StageReport("enrichment", "cache", compared=10, matched=4),
        ],
    )
    rendered = report.render()

    assert "identical" in rendered
    assert "tolerance" in rendered
    assert "cache" in rendered
    assert "30 days" in rendered


def test_an_empty_stage_is_agreement_one_not_a_division_by_zero():
    """A window with no articles is an ordinary state — a fresh clone, or a quiet interval.
    Nothing compared is nothing disagreeing."""
    assert StageReport("normalize", "identical", compared=0, matched=0).agreement == 1.0


def test_shadow_tables_live_in_their_own_namespace():
    """Re-running into the live tables would make the test pass by overwriting the thing it
    was meant to check."""
    assert _shadow("silver.articles") == "repro.articles"
    assert _shadow("silver.entity_mentions") == "repro.entity_mentions"


# --- the real replay ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-repro", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.mark.spark
def test_co_members_describes_the_partition_not_the_labels(spark):
    """Two runs that group the same articles but name the groups differently have
    reproduced the clustering. Comparing `cluster_id` would call that a failure, which
    would make the number describe id generation rather than the algorithm."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS probe")
    spark.sql("DROP TABLE IF EXISTS probe.map_a PURGE")
    spark.sql("DROP TABLE IF EXISTS probe.map_b PURGE")
    spark.sql("CREATE TABLE probe.map_a (article_id string, cluster_id string) USING iceberg")
    spark.sql("CREATE TABLE probe.map_b (article_id string, cluster_id string) USING iceberg")
    spark.sql("INSERT INTO probe.map_a VALUES ('a','c1'),('b','c1'),('c','c2')")
    # Same grouping, different names.
    spark.sql("INSERT INTO probe.map_b VALUES ('a','zz'),('b','zz'),('c','yy')")

    assert _co_members(spark, "probe.map_a") == _co_members(spark, "probe.map_b")


@pytest.mark.spark
def test_a_genuinely_different_grouping_is_caught(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS probe")
    spark.sql("DROP TABLE IF EXISTS probe.map_c PURGE")
    spark.sql("DROP TABLE IF EXISTS probe.map_d PURGE")
    spark.sql("CREATE TABLE probe.map_c (article_id string, cluster_id string) USING iceberg")
    spark.sql("CREATE TABLE probe.map_d (article_id string, cluster_id string) USING iceberg")
    spark.sql("INSERT INTO probe.map_c VALUES ('a','c1'),('b','c1'),('c','c2')")
    spark.sql("INSERT INTO probe.map_d VALUES ('a','c1'),('b','c2'),('c','c2')")

    left, right = _co_members(spark, "probe.map_c"), _co_members(spark, "probe.map_d")
    disagreeing = [a for a in left if left[a] != right[a]]
    assert sorted(disagreeing) == ["a", "b", "c"]


@pytest.mark.spark
def test_bronze_bytes_reproduce_over_a_real_committed_window(spark, tmp_path):
    """The one stage whose failure means storage corruption rather than a code change."""
    from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
    from signal_core.hashing import content_hash
    from signal_core.ops.reproduce import check_bronze_bytes
    from signal_core.spark.jobs.commit_bronze import commit
    from signal_core.staging import write_staging
    from signal_core.timeutil import utc_now

    spark.sql("DROP TABLE IF EXISTS bronze.raw_documents PURGE")
    now = utc_now()
    payloads = [b'{"a": 1}', b'{"b": 2}', b"<rss>3</rss>"]
    docs = [
        RawDocument(
            ingest_id=f"repro-{i}",
            source_id="fake",
            fetched_at=now,
            source_url=f"https://example.test/{i}",
            http_status=200,
            outcome=FetchOutcome.OK,
            etag=None,
            last_modified=None,
            content_hash=content_hash(payload),
            payload=payload,
            payload_format=PayloadFormat.JSON,
            latency_ms=1,
            byte_count=len(payload),
        )
        for i, payload in enumerate(payloads)
    ]
    write_staging(docs, tmp_path / "staging")
    commit(spark, tmp_path / "staging", table="bronze.raw_documents")

    report = check_bronze_bytes(spark, now - timedelta(minutes=1), now + timedelta(minutes=1))

    assert report.claim == "identical"
    assert report.compared == 3
    assert report.passed
    assert report.mismatched == 0


@pytest.mark.spark
def test_a_corrupted_payload_is_reported_with_its_ingest_id(spark, tmp_path):
    """A count alone is not a diagnosis. The failing ids are what makes a corruption
    report actionable."""
    from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
    from signal_core.ops.reproduce import check_bronze_bytes
    from signal_core.spark.jobs.commit_bronze import commit
    from signal_core.staging import write_staging
    from signal_core.timeutil import utc_now

    spark.sql("DROP TABLE IF EXISTS bronze.raw_documents PURGE")
    now = utc_now()
    payload = b'{"real": 1}'
    doc = RawDocument(
        ingest_id="repro-corrupt",
        source_id="fake",
        fetched_at=now,
        source_url="https://example.test/x",
        http_status=200,
        outcome=FetchOutcome.OK,
        etag=None,
        last_modified=None,
        # Deliberately not the hash of the payload — the shape a bit-flip would leave.
        content_hash="0" * 64,
        payload=payload,
        payload_format=PayloadFormat.JSON,
        latency_ms=1,
        byte_count=len(payload),
    )
    write_staging([doc], tmp_path / "staging")
    commit(spark, tmp_path / "staging", table="bronze.raw_documents")

    report = check_bronze_bytes(spark, now - timedelta(minutes=1), now + timedelta(minutes=1))

    assert not report.passed
    assert report.mismatched == 1
    assert report.examples == ("repro-corrupt",)
