"""SPEC §7.4's novelty component. ADR-0009, ADR-0016; docs/runbooks/phase-5.md 5.C.

No network and no Ollama: `score_novelty` takes vectors, so everything here is arithmetic on
fixed ones. The encoder's behaviour is measured in `evals/experiments/novelty_floor.py`, which
is an experiment against the real corpus rather than a test.
"""

from __future__ import annotations

import math

import pytest

from signal_core.brief.novelty import SIM_FLOOR, novelty_from_similarity, score_novelty
from signal_core.brief.ranker import WEIGHTS, score_cluster
from signal_core.enrich.embed import max_similarity, normalize, text_key


def unit(*values: float) -> list[float]:
    return normalize(list(values))


@pytest.mark.parametrize(
    ("similarity", "expected"),
    [(1.0, 0.0), (0.75, 0.5), (SIM_FLOOR, 1.0), (0.2, 1.0), (1.5, 0.0)],
)
def test_similarity_rescales_into_a_clamped_score(similarity, expected):
    assert novelty_from_similarity(similarity) == pytest.approx(expected)


def test_an_exact_repeat_scores_zero_novelty():
    """The case this component exists for: 37.3% of a real window is a near-exact repeat of
    something in the prior 30 days (`evals/experiments/novelty_floor.py`)."""
    vector = unit(1.0, 0.0, 0.0)
    scores = score_novelty([{"cluster_id": "c1"}], [vector], [vector])
    assert scores["c1"] == pytest.approx(0.0)


def test_something_unlike_anything_recent_scores_full_novelty():
    scores = score_novelty([{"cluster_id": "c1"}], [unit(1.0, 0.0)], [unit(0.0, 1.0)])
    assert scores["c1"] == pytest.approx(1.0)


def test_the_nearest_history_item_wins_not_the_average():
    """A story is recycled if it repeats *anything* recent, not if it repeats everything."""
    target = unit(1.0, 0.0)
    history = [unit(0.0, 1.0), unit(0.0, 1.0), target]
    assert score_novelty([{"cluster_id": "c1"}], [target], history)["c1"] == pytest.approx(0.0)


def test_no_history_returns_no_scores_rather_than_maximal_ones():
    """SPEC §17: never invent a metric. A fresh lake has not established that anything is
    new, so the component records "not measured" instead of "measured as maximal"."""
    assert score_novelty([{"cluster_id": "c1"}], [unit(1.0, 0.0)], []) == {}


# --- how it reaches the score -----------------------------------------------------------


def _cluster(**overrides):
    from datetime import UTC, datetime

    base = {
        "cluster_id": "c1",
        "distinct_publisher_count": 1,
        "fetched_at": datetime(2026, 8, 29, 12, tzinfo=UTC),
        "last_seen": datetime(2026, 8, 29, 12, tzinfo=UTC),
        "entities": [],
        "title": "T",
    }
    return base | overrides


def test_novelty_reaches_the_score_and_is_recorded_in_the_components():
    from datetime import UTC, datetime

    now = datetime(2026, 8, 29, 12, tzinfo=UTC)
    scored = score_cluster(_cluster(), now, novelty={"c1": 1.0})
    assert scored["score_components"]["novelty"] == 1.0
    assert scored["score"] >= WEIGHTS["novelty"]


def test_an_unmeasured_novelty_is_zero_not_absent():
    """A missing key in `score_components` would read as an error rather than a zero, which
    is the invariant the whole map is built on."""
    from datetime import UTC, datetime

    scored = score_cluster(_cluster(), datetime(2026, 8, 29, 12, tzinfo=UTC))
    assert scored["score_components"]["novelty"] == 0.0
    assert set(scored["score_components"]) == set(WEIGHTS)


def test_a_recycled_story_ranks_below_an_identical_but_novel_one():
    """The behaviour the component was added for, asserted end to end through the ranker."""
    from datetime import UTC, datetime

    now = datetime(2026, 8, 29, 12, tzinfo=UTC)
    novelty = {"recycled": 0.0, "fresh": 1.0}
    recycled = score_cluster(_cluster(cluster_id="recycled"), now, novelty=novelty)
    fresh = score_cluster(_cluster(cluster_id="fresh"), now, novelty=novelty)
    assert fresh["score"] > recycled["score"]


# --- the cache key ------------------------------------------------------------------------


def test_the_cache_key_is_the_text_not_the_cluster():
    """A rolling 72-hour window re-clusters the same headline on three consecutive days under
    three ids. Keying on the text is what makes those one inference instead of three."""
    assert text_key("Northwind acquires Lumen", "d1") == text_key(
        "Northwind   acquires Lumen", "d1"
    )


def test_a_different_encoder_is_a_different_key():
    """A vector from another model is an answer to a different question, not a cheaper answer
    to the same one — mixing them would put two coordinate systems in one cosine."""
    assert text_key("Northwind", "digest-a") != text_key("Northwind", "digest-b")


def test_max_similarity_of_an_empty_corpus_is_zero():
    assert max_similarity(unit(1.0, 0.0), []) == 0.0


def test_normalize_makes_a_unit_vector():
    assert math.isclose(sum(x * x for x in normalize([3.0, 4.0])), 1.0)
    assert normalize([0.0, 0.0]) == [0.0, 0.0]
