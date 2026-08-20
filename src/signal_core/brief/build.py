"""The daily brief, built from the real lake. SPEC §12's brief ladder, rung 3.0.

The analogue of `skeleton.run` — same five-stage shape, same ranker, same renderer, same
health footer — with two differences: the articles are real, and they arrive over Athena
instead of from a fake poller.

Rung 3.0 deliberately keeps Phase 0's clustering (`dedup.exact_dedup` + `group_stories`,
union-find over all pairs) rather than waiting for 3.B's Spark job. That is the point of
the ladder (ADR-0008 §2): §1's success criterion is behavioural — read daily for a month —
and calendar time is the one input that cannot be compressed by working harder. So the
reading starts against a page that is honestly rough, and every later rung improves
something already being read.

It also produces the measurement that justifies 3.B. `group_stories` is O(n^2) and says so
(`dedup.py`); this is where that stops being a theoretical note and becomes a number, which
is why the cluster stage is timed separately below.
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
    read_articles,
    read_health,
)
from signal_core.brief.render import write_brief
from signal_core.config import Settings
from signal_core.dedup import exact_dedup, group_stories
from signal_core.ops.health import RunHealth
from signal_core.timeutil import utc_now


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

    print(f"[1/4] read   — silver.articles, {window_hours}h window from {since:%Y-%m-%d %H:%M}Z")
    articles, articles_query = read_articles(since, now, client=client)
    print(
        f"        {len(articles)} articles, "
        f"{articles_query.bytes_scanned:,} bytes scanned, ${articles_query.cost_usd:.6f}"
    )

    print("[2/4] health — ops.source_health, newest verdict per source")
    healths, health_query = read_health(now - timedelta(hours=HEALTH_LOOKBACK_HOURS), client=client)
    degraded = [h.source_id for h in healths if h.status != "ok"]
    print(f"        {len(healths)} sources, {len(degraded)} not ok: {', '.join(degraded) or '—'}")

    print("[3/4] cluster— exact dedup + near-duplicate grouping")
    clustering_started = time.monotonic()
    deduped, exact_removed = exact_dedup(articles)
    grouped = group_stories(deduped)
    clusters = grouped.clusters
    clustering_seconds = time.monotonic() - clustering_started
    # The number 3.B exists to fix. `group_stories` compares every surviving pair, so this
    # grows with the square of the window; record it rather than round it away.
    print(
        f"        {len(articles)} in -> {len(clusters)} clusters "
        f"({exact_removed} exact dupes) in {clustering_seconds:.1f}s "
        f"[{len(deduped) * max(len(deduped) - 1, 0) // 2:,} pairs compared]"
    )
    if grouped.dissolved:
        # Loud on purpose. A dissolved cluster means the same-story rule chained a false
        # merge across a large component, and a run that hid that would look identical to a
        # clean one (SPEC §11).
        print(
            f"        WARNING: {grouped.dissolved} oversized cluster(s) dissolved, "
            f"{grouped.dissolved_articles} articles returned to singletons"
        )

    print("[4/4] brief  — rank + render")
    ranked = rank(clusters, limit=limit, now=now)
    health = RunHealth(
        sources=healths,
        articles_in=len(articles),
        clusters_out=len(clusters),
        exact_duplicates_removed=exact_removed,
        cache_hit_rate=0.0,  # Phase 4B's enrichment cache; nothing to report yet.
        runtime_seconds=time.monotonic() - started,
        bytes_scanned=articles_query.bytes_scanned + health_query.bytes_scanned,
        estimated_cost_usd=articles_query.cost_usd + health_query.cost_usd,
    )
    path = write_brief(ranked, health, settings.out_root, date=date)
    print(f"        {path}  [{health.status}]")
    return path
