"""The daily brief, built from the real lake. SPEC §12's brief ladder.

The brief reads **`silver.story_clusters` and `silver.entity_mentions`** — the tables 3.B and
3.C write — instead of re-clustering `silver.articles` in this process. That was the whole of
3.D, and the reason it matters is not performance:

**The thing being read every morning is now the thing being measured.** Rung 3.0 shipped
Phase 0's in-process clustering so that reading could start before the Spark job existed
(ADR-0008 §2). The cost of that was a fork: `make eval` scored `dedup.decide` at the
thresholds 3.B fitted, while the brief ran the same function over a different code path with
no blocking, no size guard, and no entity resolution at all. Two implementations of "what is
a story" and only one of them was under test. That collapsed them.

What that bought, concretely:

- **The size guard applies.** `group_stories` in-process had no `MAX_CLUSTER_SHARE`
  dissolution until 3.B added it, and 3.0's very first real brief led with a 1,720-article
  phantom cluster (`docs/runbooks/phase-3.md` 3.0).
- **Entities appear**, because they exist in a table now rather than being derivable only by
  running the resolver over every article at render time.
- **The morning read stops costing a quadratic scan.** 3.0 measured 3.58M pairwise
  comparisons per brief; the count is now zero, because the comparing happened at 05:00 in
  the job built to do it.

## Rung 4B — enrichment

The read-and-rank moved to `brief/select.py` because 4B's enrichment stage needs exactly the
same ranked window, and two copies of it would drift the first time a component was added to
`WEIGHTS`. The brief then reads what that stage wrote.

**The brief never calls the model.** It reads `gold.cluster_enrichment` and renders what is
there, so a morning where Ollama was off produces a brief without summaries rather than no
brief — the same degradation `select.optional_read` gives every other additive read. This is
also why the 16:00 path stays JVM-free and inference-free: `enrich_dag` runs earlier and the
cache is warm by the time this runs.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from signal_core.brief.items import write_brief_items
from signal_core.brief.read import (
    CLUSTER_WINDOW_HOURS,
    HEALTH_LOOKBACK_HOURS,
    read_health,
    read_macro_revisions,
)
from signal_core.brief.render import write_brief
from signal_core.brief.select import optional_read, ranked_window
from signal_core.config import Settings
from signal_core.ops.health import RunHealth
from signal_core.timeutil import BRIEF_TZ, brief_date, ensure_utc, utc_now

# A brief is **stale** when the newest clustered window ended before the brief's own date —
# that is, when the daily chain has not finished today. A date comparison rather than an age
# in hours, because an hours threshold could not express the fault it was there to catch: the
# previous 36-hour bound let a brief built on the previous day's clustering pass without
# comment, which is precisely the late-wake case (ADR-0014). `cluster` is asset-triggered now
# and has no clock of its own, so "36 hours since the 05:00 run" no longer means anything.
#
# This exists because **an empty brief and a stale brief look identical on the page**, and
# they are opposite faults: empty means ingestion stopped, stale means the chain did.
# SPEC §11's argument is that silence is the failure mode; a brief that renders yesterday's
# stories under today's date without comment is exactly that.
#
# **Marked on the page, not withheld.** ADR-0014 halts the chain on failure because a stale
# brief is undetectable — but the fix for undetectable is to make it detectable, and a brief
# the reader can see is wrong beats one that never arrives. The health table, costs and macro
# revisions in it are still true; only the stories are old, and now they say so.


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
    # Resolved here rather than left to `write_brief`'s default, because the staleness check
    # below compares against it and has to mean the same date the header will carry.
    brief_day = date or brief_date(now)
    # Set only when the clustered window predates `brief_day`; threaded into the template so
    # the page itself carries the warning rather than only this task's log.
    stale_since: str | None = None

    print(
        f"[1/4] window — silver.story_clusters + entities + signals "
        f"(articles from {since:%m-%d %H:%M}Z)"
    )
    window = ranked_window(
        limit=limit, window_hours=window_hours, now=now, client=client, progress=print
    )
    ranked = window.clusters
    cluster_read = window.cluster_read
    clusters = cluster_read.clusters
    print(
        f"        {len(clusters)} clusters, {cluster_read.articles_in} articles, "
        f"{window.cluster_query.bytes_scanned:,} bytes scanned, "
        f"${window.cluster_query.cost_usd:.6f}"
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
        window_end_local = ensure_utc(cluster_read.window_end).astimezone(BRIEF_TZ)
        if window_end_local.strftime("%Y-%m-%d") < brief_day:
            # Not "old" in the abstract — older than the edition it is being rendered into.
            stale_since = window_end_local.strftime("%Y-%m-%d %H:%M %Z")
            print(
                f"        WARNING: newest clustered window ended {stale_since}, before this "
                f"brief's date ({brief_day}) — the daily chain has not run today. "
                f"{window_age:.0f}h old; the brief will say so on the page."
            )
    print(
        f"        {window.linked_entities} company links across {len(window.entities)} "
        f"clusters, {len(window.velocity_slopes)} with a slope, "
        f"{len(window.market_moves)} tickers priced, {len(window.feedback)} marks"
    )

    print("[2/4] health — ops.source_health, newest verdict per source")
    healths, health_query = read_health(now - timedelta(hours=HEALTH_LOOKBACK_HOURS), client=client)
    degraded = [h.source_id for h in healths if h.status != "ok"]
    print(f"        {len(healths)} sources, {len(degraded)} not ok: {', '.join(degraded) or '—'}")

    print("[3/4] enrich — gold.cluster_enrichment + gold.macro_observations")
    shown = [c for c in ranked if c.get("included")]
    enrichment, enrichment_query = optional_read(
        lambda: _read_enrichment(shown, settings=settings, client=client),
        warning=(
            "no gold.cluster_enrichment yet — has the enrich DAG run? "
            "Stories are unaffected; summaries and topics are missing from this brief."
        ),
        progress=print,
    )
    enrichment = enrichment or {}
    for cluster in ranked:
        found = enrichment.get(cluster["cluster_id"])
        cluster["summary"] = found.summary if found else None
        cluster["topic"] = found.topic if found else None
    # The share of *shown* stories the enrichment cache could answer for. Deliberately not
    # the same number `enrich/run.py` reports: that one is the share of clusters it enriched
    # without calling the model, which is a fact about inference cost. This one is a fact
    # about what the reader is looking at, which is what belongs in the reader's footer.
    covered = sum(1 for c in shown if c.get("summary"))
    coverage_rate = covered / len(shown) if shown else 0.0
    print(f"        {covered}/{len(shown)} shown stories enriched")

    revisions, revision_query = optional_read(
        lambda: read_macro_revisions(now, client=client),
        warning="no gold.macro_observations yet — has the macro DAG run? (4B.I)",
        progress=print,
    )
    revisions = revisions or []
    print(f"        {len(revisions)} macro revisions in the last 45 days")

    print("[4/4] brief  — render + record")
    charged = (*window.queries, health_query, enrichment_query, revision_query)
    health = RunHealth(
        sources=healths,
        articles_in=cluster_read.articles_in,
        clusters_out=len(clusters),
        # Unknown here, and deliberately not zero. Exact-duplicate collapsing happens inside
        # `cluster_window`, which reports it as a task return value the DAG surfaces; the
        # brief no longer computes it and printing a 0 would be a fiction of the kind SPEC
        # §17 rules out. `None` renders as "—".
        exact_duplicates_removed=None,
        enrichment_coverage=coverage_rate,
        runtime_seconds=time.monotonic() - started,
        # Every query the morning made, charged and reported. A component that reads a table
        # the reader is not told about is a cost SPEC §10.3 would not see.
        bytes_scanned=sum(q.bytes_scanned for q in charged),
        estimated_cost_usd=sum(q.cost_usd for q in charged),
    )
    path = write_brief(
        ranked,
        health,
        settings.out_root,
        date=date,
        revisions=revisions,
        stale_since=stale_since,
    )

    # Written after rendering, not before: the row records what the reader actually saw,
    # including `included`, which is a property of the cut rather than of the score. This is
    # also what `signal brief feedback` updates and what the next run's `feedback` component
    # reads back (SPEC §9's `brief_items`).
    written = write_brief_items(ranked, date=date, now=now, client=client)
    print(f"        {path}  [{health.status}], {written} items recorded")
    return path


def _read_enrichment(
    clusters: list[dict[str, Any]], *, settings: Settings, client: Any | None
) -> tuple[dict[str, Any], Any]:
    """Imported lazily so `brief.build` does not pull the enrichment package at import time.

    Not a packaging constraint — `enrich/` imports only httpx and pydantic — but an import
    ordering one: `enrich.run` imports `brief.select`, and keeping this call inside the
    function keeps the dependency one-directional at module scope.
    """
    from signal_core.enrich.run import read_for_clusters

    return read_for_clusters(clusters, settings=settings, client=client)
