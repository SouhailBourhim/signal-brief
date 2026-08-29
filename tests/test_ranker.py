"""SPEC §7.4's components, each in isolation. 4A.H.

The scoring is a claim about what makes a story worth reading, so each component is pinned
to the defect or requirement it exists for rather than to a number that happened to fall out.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from signal_core.brief.ranker import (
    MARKET_MOVE_SIGMA,
    VELOCITY_SATURATION,
    WEIGHTS,
    rank,
    score_cluster,
)
from signal_core.watchlist import Watchlist

NOW = datetime(2026, 8, 22, 6, 0, tzinfo=UTC)


def _cluster(**overrides):
    base = {
        "cluster_id": "c1",
        "title": "Northwind acquires Lumen Robotics",
        "body_text": "",
        "distinct_publisher_count": 1,
        "fetched_at": NOW - timedelta(hours=1),
        "last_seen": NOW - timedelta(hours=1),
        "entities": [],
    }
    return {**base, **overrides}


def _watchlist(*, companies=(), technologies=()):
    return Watchlist(
        companies=frozenset(companies), technologies=tuple(technologies), macro_series=()
    )


# --- the weights themselves ------------------------------------------------------------


def test_weights_sum_to_one():
    """So a score reads as a fraction of the best possible story rather than an arbitrary
    magnitude, and so adding a component forces a deliberate re-split rather than silently
    inflating every score."""
    assert sum(WEIGHTS.values()) == 1.0


def test_novelty_is_a_weighted_component_and_completes_spec_7_4():
    """The inverse of the assertion this file carried through 4A and 4B.

    It used to read `assert "novelty" not in WEIGHTS`, pinning ADR-0009's deferral: every
    embedding was behind Ollama, and a lexical stand-in would have scored near chance while
    occupying a weight. 5.C landed the Ollama vehicle (ADR-0016), so the pin inverts rather
    than being deleted — the reason it existed is the reason it can now go.
    """
    assert "novelty" in WEIGHTS
    assert set(WEIGHTS) == {
        "novelty",
        "breadth",
        "velocity",
        "relevance",
        "market_corroboration",
        "feedback",
        "recency",
    }


def test_breadth_is_a_tiebreaker_not_a_quarter_of_the_score():
    """5.C measured 99.64% of clusters holding exactly one publisher — 1,674 of 1,680.

    A quarter of the score was resting on a signal this source mix emits for 0.36% of
    clusters. The weight goes back up when wire sources land, with the measurement that
    raises it.
    """
    assert WEIGHTS["breadth"] == 0.05
    assert WEIGHTS["breadth"] < WEIGHTS["novelty"]


def test_every_weighted_component_is_explained():
    """SPEC §7.4: a ranking decision has to stay explainable after the fact."""
    scored = score_cluster(_cluster(), now=NOW)
    assert set(scored["score_components"]) == set(WEIGHTS)


# --- relevance, which is also 3.E's salience fix ---------------------------------------


def test_relevance_scores_the_subject_not_a_photo_credit():
    """3.E's salience finding, carried into 4A by SPEC §12: the brief showed every resolved
    mention as a subject, so a photo credit put Getty Images on an Amazon story.

    `read_cluster_entities` returns entities most-mentioned first, so the subject is the
    first one and an incidental credit is not."""
    watchlist = _watchlist(companies=["AMZN"])

    subject = _cluster(
        entities=[
            {"entity_id": "AMZN", "ticker": "AMZN", "mentions": 9},
            {"entity_id": "GETY", "ticker": "GETY", "mentions": 1},
        ]
    )
    credit_only = _cluster(
        entities=[
            {"entity_id": "GETY", "ticker": "GETY", "mentions": 1},
            {"entity_id": "AMZN", "ticker": "AMZN", "mentions": 1},
        ]
    )

    subject_score = score_cluster(subject, now=NOW, watchlist=watchlist)
    credit_score = score_cluster(credit_only, now=NOW, watchlist=watchlist)

    assert subject_score["score_components"]["relevance"] == 1.0
    # Still counted — a watchlist company mentioned in passing is weak evidence, not none —
    # but decisively below being the subject.
    assert (
        credit_score["score_components"]["relevance"]
        < subject_score["score_components"]["relevance"]
    )


def test_a_technology_keyword_is_a_topic_not_a_subject():
    watchlist = _watchlist(companies=["NVDA"], technologies=["inference"])
    topic = _cluster(title="A cheaper inference runtime")
    subject = _cluster(entities=[{"entity_id": "NVDA", "ticker": "NVDA", "mentions": 4}])

    topic_relevance = score_cluster(topic, now=NOW, watchlist=watchlist)["score_components"][
        "relevance"
    ]
    subject_relevance = score_cluster(subject, now=NOW, watchlist=watchlist)["score_components"][
        "relevance"
    ]
    assert 0 < topic_relevance < subject_relevance


def test_an_unrelated_story_scores_zero_relevance():
    scored = score_cluster(_cluster(), now=NOW, watchlist=_watchlist(companies=["NVDA"]))
    assert scored["score_components"]["relevance"] == 0.0


# --- velocity ---------------------------------------------------------------------------


def test_velocity_scales_with_the_slope():
    slow = score_cluster(_cluster(), now=NOW, velocity_slopes={"c1": VELOCITY_SATURATION / 3})
    fast = score_cluster(_cluster(), now=NOW, velocity_slopes={"c1": VELOCITY_SATURATION})
    assert 0 < slow["score_components"]["velocity"] < fast["score_components"]["velocity"]
    assert fast["score_components"]["velocity"] == 1.0


def test_a_cluster_with_no_snapshots_scores_zero_not_an_error():
    """Most clusters have no Hacker News member. Absence of a velocity signal is not
    evidence of a stalled story, but it is not evidence of a climbing one either."""
    scored = score_cluster(_cluster(), now=NOW, velocity_slopes={})
    assert scored["score_components"]["velocity"] == 0.0


def test_a_falling_score_floors_at_zero_rather_than_subtracting():
    """Only the reader's own mark is allowed to push a story down."""
    scored = score_cluster(_cluster(), now=NOW, velocity_slopes={"c1": -50.0})
    assert scored["score_components"]["velocity"] == 0.0


# --- market corroboration ----------------------------------------------------------------


def test_an_ordinary_days_move_is_not_corroboration():
    """The ratio is |return| / trailing stddev, so 1.0 is a normal day."""
    cluster = _cluster(entities=[{"entity_id": "NVDA", "ticker": "NVDA", "mentions": 5}])
    scored = score_cluster(cluster, now=NOW, market_moves={"NVDA": 1.0})
    assert scored["score_components"]["market_corroboration"] == 0.0


def test_a_move_past_the_threshold_scales_up():
    cluster = _cluster(entities=[{"entity_id": "NVDA", "ticker": "NVDA", "mentions": 5}])
    modest = score_cluster(cluster, now=NOW, market_moves={"NVDA": MARKET_MOVE_SIGMA * 1.5})
    extreme = score_cluster(cluster, now=NOW, market_moves={"NVDA": MARKET_MOVE_SIGMA * 4})
    modest_score = modest["score_components"]["market_corroboration"]
    extreme_score = extreme["score_components"]["market_corroboration"]
    assert 0 < modest_score < extreme_score == 1.0


def test_market_corroboration_reads_the_subject_not_an_incidental_mention():
    """Same salience rule as relevance: a photo credit's ticker moving says nothing about
    the story it was credited on."""
    cluster = _cluster(
        entities=[
            {"entity_id": "AMZN", "ticker": "AMZN", "mentions": 9},
            {"entity_id": "GETY", "ticker": "GETY", "mentions": 1},
        ]
    )
    scored = score_cluster(cluster, now=NOW, market_moves={"GETY": 10.0})
    assert scored["score_components"]["market_corroboration"] == 0.0


# --- feedback ----------------------------------------------------------------------------


def test_a_thumbs_down_subtracts():
    """The one component that can go negative. "I did not want this" should push a story
    down, not merely fail to push it up."""
    up = score_cluster(_cluster(), now=NOW, feedback={"c1": 1.0})
    down = score_cluster(_cluster(), now=NOW, feedback={"c1": -1.0})
    assert up["score"] > down["score"]
    assert down["score_components"]["feedback"] == -1.0


def test_no_mark_is_neutral():
    scored = score_cluster(_cluster(), now=NOW, feedback={})
    assert scored["score_components"]["feedback"] == 0.0


# --- breadth, and what 3.D fixed ---------------------------------------------------------


def test_a_single_publisher_has_no_breadth():
    """3.D: `count / 4` gave every singleton 0.25 and a floor nothing could fall below, and
    nine of ten stories on the page became SEC form numbers."""
    scored = score_cluster(_cluster(distinct_publisher_count=1), now=NOW)
    assert scored["score_components"]["breadth"] == 0.0


def test_relevance_can_outrank_a_fresh_single_publisher_filing():
    """The shape of the whole 4A weighting, asserted end to end. 3.D found minutes-old EDGAR
    filings beating corroborated stories hours old, and said the fix was competition from
    components that measure importance rather than freshness."""
    watchlist = _watchlist(companies=["NVDA"])
    filing = _cluster(
        cluster_id="filing",
        title="4 - Some Officer (0001234567) (Reporting)",
        distinct_publisher_count=1,
        last_seen=NOW,  # brand new
    )
    story = _cluster(
        cluster_id="story",
        title="NVDA announces a new datacenter part",
        distinct_publisher_count=3,
        last_seen=NOW - timedelta(hours=5),
        entities=[{"entity_id": "NVDA", "ticker": "NVDA", "mentions": 6}],
    )

    ranked = rank([filing, story], limit=10, now=NOW, watchlist=watchlist)
    assert ranked[0]["cluster_id"] == "story"


def test_rank_marks_what_was_included():
    clusters = [_cluster(cluster_id=f"c{i}") for i in range(5)]
    ranked = rank(clusters, limit=2, now=NOW)
    assert [c["included"] for c in ranked] == [True, True, False, False, False]
    assert [c["rank"] for c in ranked] == [1, 2, 3, 4, 5]
