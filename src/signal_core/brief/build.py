"""The daily brief, built from the real lake. SPEC §12's brief ladder, rung 3.x.

The brief now reads **`silver.story_clusters` and `silver.entity_mentions`** — the tables
3.B and 3.C write — instead of re-clustering `silver.articles` in this process. That is the
whole of 3.D, and the reason it matters is not performance:

**The thing being read every morning is now the thing being measured.** Rung 3.0 shipped
Phase 0's in-process clustering so that reading could start before the Spark job existed
(ADR-0008 §2). The cost of that was a fork: `make eval` scored `dedup.decide` at the
thresholds 3.B fitted, while the brief ran the same function over a different code path with
no blocking, no size guard, and no entity resolution at all. Two implementations of "what is
a story" and only one of them was under test. This collapses them.

What that buys, concretely:

- **The size guard applies.** `group_stories` in-process had no `MAX_CLUSTER_SHARE`
  dissolution until 3.B added it, and 3.0's very first real brief led with a 1,720-article
  phantom cluster (`docs/runbooks/phase-3.md` 3.0).
- **Entities appear**, because they exist in a table now rather than being derivable only by
  running the resolver over every article at render time.
- **The morning read stops costing a quadratic scan.** 3.0 measured 3.58M pairwise
  comparisons per brief; the count is now zero, because the comparing happened at 05:00 in
  the job built to do it.

The stages are otherwise unchanged — same ranker, same renderer, same health footer — which
is the ladder working as designed: each rung improves something already being read.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from signal_core.brief.ranker import rank
from signal_core.brief.read import (
    CLUSTER_WINDOW_HOURS,
    HEALTH_LOOKBACK_HOURS,
    read_cluster_entities,
    read_clusters,
    read_health,
)
from signal_core.brief.render import write_brief
from signal_core.config import Settings
from signal_core.ops.athena import AthenaQueryFailed, QueryResult
from signal_core.ops.health import RunHealth
from signal_core.timeutil import utc_now

# How stale the newest clustered window may be before the brief says so out loud. The
# cluster DAG runs daily at 05:00, so anything past a day and a half means a run was missed.
#
# This exists because **an empty brief and a stale brief look identical on the page**, and
# they are opposite faults: empty means ingestion stopped, stale means the cluster job did.
# SPEC §11's argument is that silence is the failure mode; a brief that renders yesterday's
# stories under today's date without comment is exactly that.
STALE_CLUSTER_HOURS = 36


# Athena's spelling of "you asked about a table that does not exist". Matched on the message
# because the API returns `FAILED` with a reason string rather than a typed error code.
_MISSING_TABLE = ("does not exist", "not found", "table_not_found")


def _read_entities(
    since: datetime, now: datetime, *, client: Any | None
) -> tuple[dict[str, list[dict[str, Any]]], QueryResult]:
    """Entities, or an honest nothing if 3.C has not run in this environment yet.

    Entities are **additive** to the brief — the stories are the product, and a fresh clone
    or a new account that has run `cluster` but not `resolve` should still get its morning
    read. So a missing table degrades to no entities plus a loud line, rather than taking
    down the page.

    Narrow on purpose: only "no such table" is swallowed. A permissions error, a malformed
    query or a workgroup cutoff still raises, because those are faults in a table that is
    supposed to be there, and quietly rendering a brief without entities would hide them.
    """
    try:
        return read_cluster_entities(since, now, client=client)
    except AthenaQueryFailed as failure:
        message = str(failure).lower()
        if not any(marker in message for marker in _MISSING_TABLE):
            raise
        print(
            "        WARNING: no entity tables yet — has the resolve DAG run? "
            "Stories are unaffected; company links are missing from this brief."
        )
        return {}, QueryResult(rows=[], bytes_scanned=0, engine_execution_ms=0, cost_usd=0.0)


def run(
    settings: Settings | None = None,
    *,
    limit: int = 10,
    window_hours: int = CLUSTER_WINDOW_HOURS,
    date: str | None = None,
    now: datetime | None = None,
    client: Any | None = None,
) -> Path:
    started = time.monotonic()
    settings = settings or Settings()
    now = now or utc_now()
    since = now - timedelta(hours=window_hours)

    print(
        f"[1/4] clusters— silver.story_clusters, newest window (articles from {since:%m-%d %H:%M}Z)"
    )
    cluster_read, cluster_query = read_clusters(since, now, client=client)
    clusters = cluster_read.clusters
    print(
        f"        {len(clusters)} clusters, {cluster_read.articles_in} articles, "
        f"{cluster_query.bytes_scanned:,} bytes scanned, ${cluster_query.cost_usd:.6f}"
    )

    if not clusters:
        # Loud, and specific about which of the two faults it is. "No stories" with no
        # further comment is the failure mode this whole footer exists to prevent.
        print(
            "        WARNING: no clusters in silver.story_clusters — has the cluster DAG "
            "run? (`make up`, then trigger `cluster`)"
        )
    elif cluster_read.window_end is not None:
        # Measured from the window's **end**, not its start. `window_start` is 72 hours
        # before the run by construction, so ages taken from it are never under 72 and the
        # warning fired on every healthy brief — which is worse than not having it, because
        # a warning that is always on is one nobody reads.
        window_age = (now - cluster_read.window_end).total_seconds() / 3600.0
        print(
            f"        window {cluster_read.window_start:%m-%d %H:%M}Z"
            f" -> {cluster_read.window_end:%m-%d %H:%M}Z"
            f", algo {cluster_read.algo_version}"
        )
        if window_age > STALE_CLUSTER_HOURS:
            print(
                f"        WARNING: newest clustered window is {window_age:.0f}h old — "
                "these are not today's stories"
            )

    print("[2/4] entities— silver.entity_mentions x article_clusters x dim_entities")
    entities, entity_query = _read_entities(since, now, client=client)
    for cluster in clusters:
        cluster["entities"] = entities.get(cluster["cluster_id"], [])
    linked = sum(len(cluster["entities"]) for cluster in clusters)
    print(
        f"        {linked} company links across {len(entities)} clusters, "
        f"{entity_query.bytes_scanned:,} bytes scanned"
    )

    print("[3/4] health  — ops.source_health, newest verdict per source")
    healths, health_query = read_health(now - timedelta(hours=HEALTH_LOOKBACK_HOURS), client=client)
    degraded = [h.source_id for h in healths if h.status != "ok"]
    print(f"        {len(healths)} sources, {len(degraded)} not ok: {', '.join(degraded) or '—'}")

    print("[4/4] brief   — rank + render")
    ranked = rank(clusters, limit=limit, now=now)
    health = RunHealth(
        sources=healths,
        articles_in=cluster_read.articles_in,
        clusters_out=len(clusters),
        # Unknown here, and deliberately not zero. Exact-duplicate collapsing happens inside
        # `cluster_window`, which reports it as a task return value the DAG surfaces; the
        # brief no longer computes it and printing a 0 would be a fiction of the kind SPEC
        # §17 rules out. `None` renders as "—".
        exact_duplicates_removed=None,
        cache_hit_rate=0.0,  # Phase 4B's enrichment cache; nothing to report yet.
        runtime_seconds=time.monotonic() - started,
        bytes_scanned=(
            cluster_query.bytes_scanned + entity_query.bytes_scanned + health_query.bytes_scanned
        ),
        estimated_cost_usd=(cluster_query.cost_usd + entity_query.cost_usd + health_query.cost_usd),
    )
    path = write_brief(ranked, health, settings.out_root, date=date)
    print(f"        {path}  [{health.status}]")
    return path
