"""SPEC §7.4's novelty component. ADR-0009, ADR-0016; docs/runbooks/phase-5.md 5.C.

> **Novelty** — Embedding distance to the last 30 days of clusters — recycled narratives sink.

Absent from `WEIGHTS` for two phases. 4A deferred it because every embedding was behind
Ollama in 4B and `test_novelty_is_not_a_weighted_component` pinned the deferral; 4B deferred
it again with ADR-0009's argument that "the infrastructure is already there" is how a ten-item
phase row gets built. This is it arriving.

## Why the raw cosine is not the score

`1 - cosine` looks like a distance and is not usable as one. Sentence embeddings of any two
English headlines sit far above zero — the smoke test that opened this work put two phrasings
of one story at 0.92 and an unrelated recipe at 0.37 — so a raw `1 - cos` would compress every
real difference into the top fifth of the scale and hand almost the whole weight to every
story regardless.

So the similarity is rescaled over the range the corpus actually occupies, and
`evals/experiments/novelty_floor.py` measured that range against 1,680 current heads and 7,011
distinct heads from the prior 30 days:

    min 0.5062 · p10 0.6439 · p25 0.7773 · p50 0.8763 · p75 1.0000 · p90 1.0000

**37.3% of a window scores at or above 0.99** — 626 of 1,680 heads are a near-exact repeat of
something already seen in the last month. That is the mass this component exists to sink, and
it is far larger than the "recycled narratives" phrasing in §7.4 suggests: a third of every
window is literally the same headline again.

`SIM_FLOOR` is 0.50 because nothing in the measured window fell below 0.5062, and because
unrelated text sits near 0.37 — so 0.50 is "as unlike anything recent as this corpus gets",
and anything below it is clamped rather than extrapolated.

The resulting component is spread rather than saturated, which is the property 4A.H said a
weighted term must have: median 0.25, p25 0.45, p10 0.71, with the 37% of exact repeats at
0.00. Compare `relevance`, which sat at 0.94-0.98 for five days because it was the only live
term — a saturated component occupying a weight is worse than an absent one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

# See the module docstring. Measured, not chosen.
SIM_FLOOR = 0.50


def novelty_from_similarity(similarity: float) -> float:
    """Rescale a cosine similarity into a 0-1 novelty score, clamped at both ends."""
    scaled = (1.0 - similarity) / (1.0 - SIM_FLOOR)
    return min(max(scaled, 0.0), 1.0)


def score_novelty(
    heads: Sequence[Mapping[str, str]],
    head_vectors: Sequence[Sequence[float]],
    history_vectors: Sequence[Sequence[float]],
) -> dict[str, float]:
    """`cluster_id` -> novelty, against the prior 30 days.

    Returns an empty map when there is no history, rather than scoring everything as maximally
    novel: a fresh lake has not established that anything is new, and awarding every story the
    full weight on a corpus that cannot yet contradict it is the kind of invented metric
    SPEC §17 rules out. `ranker` treats an absent entry as 0.0 and the component is then
    uniform, so ordering is unaffected either way — but the number that lands in
    `gold.brief_items` says "not measured" rather than "measured as maximal".
    """
    from signal_core.enrich.embed import max_similarity

    if not history_vectors:
        return {}
    return {
        head["cluster_id"]: novelty_from_similarity(max_similarity(vector, history_vectors))
        for head, vector in zip(heads, head_vectors, strict=True)
    }
