from __future__ import annotations

from datetime import UTC, datetime, timedelta

from signal_core.brief.ranker import rank, score_cluster
from signal_core.brief.render import render_brief
from signal_core.ops.health import RunHealth, assess_source


def _cluster(**overrides):
    now = datetime.now(UTC)
    base = {
        "cluster_id": "c1",
        "canonical_article_id": "c1",
        "title": "A title",
        "body_text": "Body.",
        "url_canonical": "https://a.com/x",
        "publisher_domain": "a.com",
        "published_at": now - timedelta(hours=1),
        "fetched_at": now,
        "first_seen": now - timedelta(hours=1),
        "last_seen": now - timedelta(hours=1),
        "article_count": 1,
        "distinct_publisher_count": 1,
        "publishers": ["a.com"],
        "timestamp_flagged": False,
        "story_key": None,
    }
    return {**base, **overrides}


def test_breadth_beats_a_lone_report():
    now = datetime.now(UTC)
    broad = score_cluster(_cluster(cluster_id="broad", distinct_publisher_count=4), now)
    lone = score_cluster(_cluster(cluster_id="lone", distinct_publisher_count=1), now)
    assert broad["score"] > lone["score"]


def test_score_components_are_retained_for_explainability():
    """SPEC §7.4: a scalar score cannot be explained after the fact."""
    scored = score_cluster(_cluster())
    assert set(scored["score_components"]) == {"breadth", "recency"}


def test_flagged_timestamp_falls_back_to_fetched_at():
    """A source's claimed time is not trusted once flagged (SPEC §6.2).

    The rule moved in 3.B.4: it used to run inside `score_cluster`, where it only ever saw
    the cluster head. It now runs per member in `dedup.trusted_timestamp`, so this asserts it
    where it lives rather than through the ranker, which no longer makes the decision.
    """
    from signal_core.dedup import trusted_timestamp

    now = datetime.now(UTC)
    lying = {"published_at": now - timedelta(days=30), "timestamp_flagged": True, "fetched_at": now}
    honest = {
        "published_at": now - timedelta(days=30),
        "timestamp_flagged": False,
        "fetched_at": now,
    }
    assert trusted_timestamp(lying) == now
    assert trusted_timestamp(honest) == now - timedelta(days=30)


def test_recency_tracks_the_latest_coverage_not_when_the_story_broke():
    """3.B.4. A cluster's head is by construction its earliest article, so ranking on the
    head made a story look older the longer it kept drawing coverage — exactly backwards.
    Measured on a real window: a cluster whose newest article was 23h old ranked as 65.6h."""
    now = datetime.now(UTC)
    developing = _cluster(
        cluster_id="developing",
        first_seen=now - timedelta(hours=60),
        last_seen=now - timedelta(hours=1),
    )
    stale = _cluster(
        cluster_id="stale",
        first_seen=now - timedelta(hours=60),
        last_seen=now - timedelta(hours=60),
    )

    assert score_cluster(developing, now)["score"] > score_cluster(stale, now)["score"]
    assert score_cluster(developing, now)["score_components"]["recency"] > 0.9
    assert score_cluster(stale, now)["score_components"]["recency"] == 0.0


def test_rank_marks_inclusion_and_omits_the_tail():
    """A brief is useful because of what it omits."""
    clusters = [_cluster(cluster_id=f"c{i}", distinct_publisher_count=i % 4 + 1) for i in range(8)]
    ranked = rank(clusters, limit=3)
    assert [c["rank"] for c in ranked] == list(range(1, 9))
    assert sum(c["included"] for c in ranked) == 3


def test_stale_but_successful_feed_is_degraded(fake_config):
    """The common failure mode: 200 OK, content frozen (SPEC §11)."""
    now = datetime.now(UTC)
    past_sla = now - timedelta(seconds=fake_config.freshness_sla_seconds + 60)
    stale = assess_source(fake_config, 5, past_sla, now)
    assert stale.status == "stale"

    healthy = assess_source(fake_config, 5, now - timedelta(seconds=30), now)
    assert healthy.status == "ok"


def test_thin_source_is_distinguished_from_stale(fake_config):
    now = datetime.now(UTC)
    thin = assess_source(fake_config, 0, now - timedelta(seconds=10), now)
    assert thin.status == "thin"


def test_never_succeeded_is_not_silently_ok(fake_config):
    assert assess_source(fake_config, 0, None).status == "never_succeeded"


def test_run_health_status_escalates():
    ok = RunHealth(articles_in=10, clusters_out=5)
    assert ok.status == "ok" and ok.dedup_ratio == 2.0
    assert RunHealth(articles_in=0, clusters_out=0).dedup_ratio == 0.0


def test_brief_renders_stories_and_health_footer():
    ranked = rank([_cluster(title="Northwind acquires Lumen", distinct_publisher_count=3)])
    health = RunHealth(articles_in=11, clusters_out=8, exact_duplicates_removed=1)
    html = render_brief(ranked, health, date="2026-08-18")

    assert "Northwind acquires Lumen" in html
    assert "2026-08-18" in html
    assert "pipeline: ok" in html  # SPEC §11 footer
    assert "11 articles in" in html
    assert "8 clusters out" in html


def test_render_escapes_hostile_titles():
    """Titles come from third-party feeds and are never trusted markup."""
    ranked = rank([_cluster(title="<script>alert(1)</script>")])
    html = render_brief(ranked, RunHealth(), date="2026-08-18")
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_render_handles_an_empty_brief():
    html = render_brief([], RunHealth(), date="2026-08-18")
    assert "No stories cleared the threshold." in html
