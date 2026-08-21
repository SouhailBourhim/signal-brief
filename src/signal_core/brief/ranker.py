"""Cluster ranking. SPEC §7.4.

Phase 0 scores on breadth and recency only. Novelty, velocity, relevance, and market
corroboration arrive with Phases 3-4 — but `score_components` is already a map, because
§7.4's actual requirement is that every ranking decision stays explainable after the
fact, and a scalar score cannot be explained retroactively.

Weights are hand-set and stay hand-set (SPEC §7.4): one reader's daily marks are
instrumentation, not a training set.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from signal_core.timeutil import ensure_utc, utc_now

WEIGHTS: dict[str, float] = {
    "breadth": 0.6,
    "recency": 0.4,
    # Phase 3+: "novelty", "velocity", "relevance", "market_corroboration", "feedback"
}


def score_cluster(cluster: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or utc_now()

    # Independent publishers, saturating: the tenth outlet reprinting a story says much
    # less than the second did.
    breadth = min(cluster["distinct_publisher_count"] / 4.0, 1.0)

    # `last_seen` — when the story was last *covered*, not when it broke. The distinction is
    # the whole point of ranking a cluster rather than an article: a story still drawing
    # coverage is fresh, however long ago the first report landed. `dedup.trusted_timestamp`
    # applies SPEC §6.2's "believe published_at unless it disagrees with fetched_at" rule per
    # member, so a flagged timestamp still falls back to what we observed ourselves — it just
    # does so for every article now, instead of only for the head.
    reference = cluster.get("last_seen") or cluster["fetched_at"]
    age_hours = max((now - ensure_utc(reference)).total_seconds() / 3600.0, 0.0)
    recency = max(0.0, 1.0 - age_hours / 24.0)

    components = {"breadth": breadth, "recency": recency}
    return {
        **cluster,
        "score": sum(WEIGHTS[k] * v for k, v in components.items()),
        "score_components": components,
    }


def rank(clusters: list[dict[str, Any]], limit: int = 10, now: datetime | None = None):
    """Score, sort, and cut.

    A brief is useful because of what it omits (SPEC §7.4), so `limit` is the product,
    not a pagination detail.
    """
    scored = [score_cluster(c, now) for c in clusters]
    scored.sort(key=lambda c: (-c["score"], c["cluster_id"]))
    for position, cluster in enumerate(scored, start=1):
        cluster["rank"] = position
        cluster["included"] = position <= limit
    return scored
