"""Cluster ranking. SPEC §7.4.

Five of §7.4's six components, as of 4A. `score_components` has been a map since Phase 0
because §7.4's actual requirement is that every ranking decision stays explainable after the
fact, and a scalar score cannot be explained retroactively.

Weights are hand-set and stay hand-set (SPEC §7.4): one reader's daily marks are
instrumentation, not a training set. §14 keeps automated fitting behind "several hundred
marked items", and the brief ladder is what makes that reachable rather than theoretical.

**Novelty is the missing sixth, and its absence is deliberate.** It needs embedding distance
against 30 days of cluster heads, and ADR-0009 placed every embedding in 4B behind Ollama
rather than paying `sentence-transformers`' 1.1 GB a phase early. A lexical stand-in was
considered and rejected: ADR-0009 measured the lexical same-story rule at 0.500 held-out
recall against embeddings' 0.909 on a strictly easier question, so a proxy would score near
chance while occupying a weight — and a hand-set weight over a near-chance component is
worse than an absent one, because the score is only explainable if every term in it means
something. It arrives in 4B with the stage that pays for it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from signal_core.timeutil import ensure_utc, utc_now
from signal_core.watchlist import Watchlist

# Hand-set, and they sum to 1.0 so a score reads as a fraction of the best possible story.
#
# The shape of this distribution is a claim about what makes a story worth reading, so it is
# worth stating: `relevance` and `breadth` lead because "is this about something I care
# about" and "did independent outlets corroborate it" are the two questions a brief exists to
# answer. `recency` matters but is deliberately no longer second — 3.D found EDGAR filings
# minutes old beating four-publisher stories four hours old, and the fix for that is
# competition from components that measure importance rather than freshness.
#
# `feedback` is the smallest for a reason SPEC §14 gives: with a handful of marks it should
# nudge, not steer.
#
# ## 5.C: novelty arrives, and `breadth` is cut from 0.25 to 0.05
#
# SPEC §7.4 says weights are hand-set and stay hand-set, so this is a hand-set change with a
# measurement behind it rather than a fit. Two things were measured against the deployed lake
# on 2026-08-29 (docs/runbooks/phase-5.md 5.C):
#
# **`breadth` cannot fire in this corpus.** 99.64% of clusters hold exactly one publisher —
# 1,674 of 1,680; six hold two; none holds three. Not a clustering bug: 64% of the corpus is
# SEC filings from one publisher and 30% is Hacker News pointing at 477 distinct domains, and
# every one of the six multi-publisher clusters is a correct ars/verge/techcrunch/HN merge. A
# quarter of the score was resting on a signal the source mix emits for 0.36% of clusters. It
# becomes a tiebreaker at 0.05, and the weight goes back up when wire sources land — with the
# measurement that raises it.
#
# **`novelty` is the term that actually varies.** 37.3% of a window is a near-exact repeat of
# something from the prior 30 days, and the rescaled component spreads across the rest
# (median 0.25, p25 0.45, p10 0.71) instead of saturating. See `brief/novelty.py`.
#
# `relevance` and `recency` are untouched: both are live and neither measurement questioned
# them. `recency` read 0.000 for five straight days before ADR-0014, which was a chain-ordering
# bug and not a weighting one.
WEIGHTS: dict[str, float] = {
    "relevance": 0.25,
    "recency": 0.20,
    "novelty": 0.20,
    "velocity": 0.10,
    "market_corroboration": 0.10,
    "feedback": 0.10,
    "breadth": 0.05,
}

# How far past its trailing volatility a move has to go to count as corroboration, and the
# span that volatility is measured over. ADR-0010 records this as a *stated design
# parameter*, not a fitted constant: there is no labeled set for "the market reacted" the way
# there is for dedup and entities, and SPEC §7.4 argues for explainable constants over tuned
# ones. `read_market_moves` returns the ratio; this is where it becomes a score.
MARKET_MOVE_SIGMA = 1.5

# Points per hour that counts as a story fully accelerating. HN's front page runs roughly
# 10-40 points an hour for a story that is climbing, so this saturates near the top of the
# ordinary range rather than at an outlier — a scale set by the largest value observed would
# score every normal story near zero.
VELOCITY_SATURATION = 30.0


def _relevance(cluster: Mapping[str, Any], watchlist: Watchlist) -> float:
    """Is this cluster about something the reader follows? SPEC §7.4.

    **Scored on the highest-mention entity, not on any resolved mention.** That is 3.E's
    salience finding, which SPEC §12 carried into 4A: the brief showed every resolved mention
    as a subject, so a photo credit put Getty Images on an Amazon story. `read_cluster_entities`
    already returns entities sorted by descending mention count, so the subject of a story is
    the first one and an incidental credit — typically `mentions=1` — is not.

    Technology keywords score lower than a watchlist company because they are a weaker claim:
    "this mentions GPUs" is a topic, "this is about NVDA" is a subject.
    """
    entities = cluster.get("entities") or []
    if entities and watchlist.has_company(entities[0].get("entity_id")):
        return 1.0

    matched = watchlist.matched_technologies(cluster.get("title"), cluster.get("body_text"))
    if matched:
        # Saturating at two: one keyword is a topic match, three is not three times better.
        return min(0.4 + 0.2 * len(matched), 0.8)

    # A watchlist company mentioned but not as the subject still counts for something — it is
    # weaker evidence, not absence of evidence.
    if any(watchlist.has_company(e.get("entity_id")) for e in entities):
        return 0.3
    return 0.0


def _velocity(cluster: Mapping[str, Any], slopes: dict[str, float]) -> float:
    """Is attention on this story accelerating? SPEC §7.4.

    Reads `silver.hn_score_snapshots` through `read_hn_velocity`, which is the whole reason
    4A.B added a second Hacker News poller: the original walks item ids forward and captures
    each story once, at score 1, so there was never a second point to take a slope against.

    A cluster with no Hacker News member scores 0 rather than being excluded. Absence of a
    velocity signal is not evidence of a stalled story, but it is also not evidence of a
    climbing one, and 0 is what "we cannot see this" has to mean in a weighted sum.

    A *falling* score also floors at 0 rather than going negative: a story losing points is
    not evidence against its importance, only the absence of evidence for it. Only the
    reader's own mark is allowed to subtract.
    """
    slope = slopes.get(cluster["cluster_id"])
    if slope is None:
        return 0.0
    return max(0.0, min(slope / VELOCITY_SATURATION, 1.0))


def _market_corroboration(cluster: Mapping[str, Any], moves: dict[str, float]) -> float:
    """Did the linked ticker move beyond its normal range? SPEC §7.4.

    Scored on the same highest-mention entity `_relevance` uses, and for the same salience
    reason: a photo credit's ticker moving says nothing about an Amazon story.

    The ratio is |latest return| / trailing stddev, so 1.0 is an ordinary day and
    `MARKET_MOVE_SIGMA` is where "beyond its normal range" starts. Scaled rather than
    stepped, so a 3-sigma move outranks a 1.6-sigma one instead of tying with it.
    """
    entities = cluster.get("entities") or []
    if not entities:
        return 0.0
    ticker = (entities[0].get("ticker") or entities[0].get("entity_id") or "").upper()
    ratio = moves.get(ticker)
    if ratio is None or ratio < MARKET_MOVE_SIGMA:
        return 0.0
    # Full credit at twice the threshold. A stock that moved 3 sigma has said everything it
    # is going to say about whether the story mattered.
    return min((ratio - MARKET_MOVE_SIGMA) / MARKET_MOVE_SIGMA, 1.0)


def score_cluster(
    cluster: Mapping[str, Any],
    now: datetime | None = None,
    *,
    watchlist: Watchlist | None = None,
    velocity_slopes: dict[str, float] | None = None,
    market_moves: dict[str, float] | None = None,
    feedback: dict[str, float] | None = None,
    novelty: dict[str, float] | None = None,
) -> dict[str, Any]:
    now = now or utc_now()

    # Independent publishers, saturating: the tenth outlet reprinting a story says much
    # less than the second did.
    #
    # **One publisher is zero breadth, not a quarter of it.** SPEC §7.4 defines this
    # component as the count of *independent* publishers, and a single publisher has no
    # independent corroboration by construction — so the scale starts at the second one.
    #
    # It was `count / 4`, which gave every singleton 0.25 and, at a 0.6 weight, a floor of
    # 0.15 that nothing could fall below. 3.D found what that does to a real brief: EDGAR
    # emits filings continuously, so there is always a batch minutes old scoring
    # `0.15 + 0.4 x 1.00 = 0.55`, and **nine of the ten stories on the page were SEC form
    # numbers** — beating a two-publisher story that was four hours old. A brief is useful
    # because of what it omits (§7.4), and it was omitting the news.
    #
    # This is a correction to what `breadth` means, not the arrival of §7.4's remaining
    # components. Novelty, velocity, relevance and market corroboration are still 4A.
    breadth = min(max(cluster["distinct_publisher_count"] - 1, 0) / 3.0, 1.0)

    # `last_seen` — when the story was last *covered*, not when it broke. The distinction is
    # the whole point of ranking a cluster rather than an article: a story still drawing
    # coverage is fresh, however long ago the first report landed. `dedup.trusted_timestamp`
    # applies SPEC §6.2's "believe published_at unless it disagrees with fetched_at" rule per
    # member, so a flagged timestamp still falls back to what we observed ourselves — it just
    # does so for every article now, instead of only for the head.
    reference = cluster.get("last_seen") or cluster["fetched_at"]
    age_hours = max((now - ensure_utc(reference)).total_seconds() / 3600.0, 0.0)
    recency = max(0.0, 1.0 - age_hours / 24.0)

    # Every component defaults to an empty source rather than being skipped, so a cluster
    # scored with no market data produces the same *shape* of explanation as one scored with
    # it — a missing key in `score_components` would read as an error rather than a zero.
    watchlist = watchlist or Watchlist(frozenset(), (), ())
    components = {
        "breadth": breadth,
        "recency": recency,
        "relevance": _relevance(cluster, watchlist),
        "velocity": _velocity(cluster, velocity_slopes or {}),
        "market_corroboration": _market_corroboration(cluster, market_moves or {}),
        # Marks are +1/-1 and every other component is 0..1, so this is the one term that
        # can subtract. That is intended: "I did not want this" should push a story down,
        # not merely fail to push it up.
        "feedback": (feedback or {}).get(cluster["cluster_id"], 0.0),
        # 0.0 when novelty was not computed — Ollama down, or a lake with no history to be
        # novel against. "We could not measure this" has to mean zero in a weighted sum for
        # the same reason `velocity` does; awarding the full weight to every story on a
        # corpus that cannot yet contradict it would be SPEC §17's invented metric. The
        # component is then uniform, so ordering is unaffected either way.
        "novelty": (novelty or {}).get(cluster["cluster_id"], 0.0),
    }
    return {
        **cluster,
        "score": sum(WEIGHTS[k] * v for k, v in components.items()),
        "score_components": components,
    }


def rank(
    clusters: Sequence[Mapping[str, Any]],
    limit: int = 10,
    now: datetime | None = None,
    *,
    watchlist: Watchlist | None = None,
    velocity_slopes: dict[str, float] | None = None,
    market_moves: dict[str, float] | None = None,
    feedback: dict[str, float] | None = None,
    novelty: dict[str, float] | None = None,
):
    """Score, sort, and cut.

    A brief is useful because of what it omits (SPEC §7.4), so `limit` is the product,
    not a pagination detail.
    """
    scored = [
        score_cluster(
            c,
            now,
            watchlist=watchlist,
            velocity_slopes=velocity_slopes,
            market_moves=market_moves,
            feedback=feedback,
            novelty=novelty,
        )
        for c in clusters
    ]
    scored.sort(key=lambda c: (-c["score"], c["cluster_id"]))
    for position, cluster in enumerate(scored, start=1):
        cluster["rank"] = position
        cluster["included"] = position <= limit
    return scored
