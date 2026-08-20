"""Pipeline health, as data. SPEC §11.

The footer at the bottom of the brief is the whole point: anyone who opens the output
sees the quality layer without reading any code. It is assembled here rather than in the
template so it can also be asserted on in tests and written to `ops.source_health`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from signal_core.contracts import SourceConfig
from signal_core.timeutil import ensure_utc, utc_now

# SPEC §11: "A source dropping 80% overnight should alert, not silently thin the brief."
VOLUME_DROP_RATIO = 0.2  # below 20% of baseline is the 80% drop §11 names

# Below this, percentage-of-baseline arithmetic is noise rather than signal: a source
# averaging 2 documents a window "drops 80%" by producing zero, which for a feed that
# mostly 304s is a completely normal window. Those sources are covered by `dead_feed`
# instead, which asks the question that actually applies to them.
VOLUME_BASELINE_MIN = 10.0

# The single definition of "this source is in trouble", imported by both the brief footer
# and `ingest_monitor`'s failing task. It lives here because the alternative — each caller
# keeping its own literal set — is how `thin` came to be a status that nothing acted on:
# `assess_source` could return it and the DAG's hardcoded set had never heard of it, so a
# source producing zero documents ran green (docs/runbooks/phase-1.md, the 1.E entry).
DEGRADED_STATUSES = frozenset({"stale", "never_succeeded", "gapped", "dead_feed", "volume_drop"})

# What fails the DAG. Everything degraded, plus `thin` — which is milder (a static floor,
# not evidence of a fault) but still means the window came back emptier than the source is
# configured to ever be, and silence is the failure mode §11 is built to catch.
FAILING_STATUSES = DEGRADED_STATUSES | {"thin"}


@dataclass
class SourceHealth:
    source_id: str
    docs_ingested: int
    expected_min: int
    last_success_at: datetime | None
    staleness_seconds: float
    status: str
    gap_reason: str | None = None
    # Distinct from `staleness_seconds`, which measures the *fetch*. This measures the
    # content, and is the only one of the two that can see a frozen-but-200 feed.
    content_staleness_seconds: float | None = None
    baseline_docs: float | None = None


def assess_source(
    config: SourceConfig,
    docs_ingested: int,
    last_success_at: datetime | None,
    now: datetime | None = None,
    gap_reason: str | None = None,
    last_content_change_at: datetime | None = None,
    baseline_docs: float | None = None,
) -> SourceHealth:
    """Classify one source against the four failure modes SPEC §11 names.

    The statuses, in the order they are checked — earlier ones subsume later ones, so a
    source that is both stale and thin reports the more fundamental fault:

    - `never_succeeded` — no successful fetch, ever. Not an outage; nothing ever worked.
    - `stale` — the *fetch* stopped working (`last_success_at` past its SLA).
    - `dead_feed` — the fetch works and the **content has not moved** past its own, much
      longer SLA. This is the failure §11 calls out as the common one — a feed returning
      200 with stale content, not a 500 — and it is invisible to `staleness_seconds`,
      which advances on a 304 and on an unchanged 200 alike. Requires the poller to
      maintain `State.last_content_change_at`; sources with no meaningful content-
      movement signal leave `content_staleness_sla_seconds` as None and skip the check.
    - `volume_drop` — output collapsed relative to this source's own recent baseline.
      Catches the degradation that keeps returning 200s with *some* content, so neither
      of the two checks above fires.
    - `thin` — below the static floor. A blunter instrument than `volume_drop` and still
      worth having: it needs no history, so it works on the first window after a deploy.
    - `gapped` — healthy *now*, with an interval that is permanently unrecoverable.
      `gap_reason` comes from a catch-up run (`ops.recovery`); SPEC §6.3 requires the
      loss be visible rather than implied by a thin day.

    Every status except `ok` fails the `ingest_monitor` DAG. A monitoring function whose
    findings do not stop anything is a dashboard, and §11 asked for an alert.
    """
    now = now or utc_now()
    staleness = (
        (now - ensure_utc(last_success_at)).total_seconds() if last_success_at else float("inf")
    )
    content_staleness = (
        (now - ensure_utc(last_content_change_at)).total_seconds()
        if last_content_change_at
        else None
    )

    content_sla = config.content_staleness_sla_seconds
    content_dead = (
        content_sla is not None
        and content_staleness is not None
        and content_staleness > content_sla
    )
    # Explicitly None-guarded rather than relying on a precomputed flag, so the
    # comparison below is well-typed and the threshold reads in one place.
    volume_dropped = (
        baseline_docs is not None
        and baseline_docs >= VOLUME_BASELINE_MIN
        # `<=`, not `<`: §11 asks for a *drop of 80%* to alert, and a source left at
        # exactly 20% of baseline has dropped exactly 80%. With `<` the boundary case —
        # 900/hour falling to 180 — read `ok`, which is the one number in the range most
        # likely to be quoted at it.
        and docs_ingested <= baseline_docs * VOLUME_DROP_RATIO
    )

    if last_success_at is None:
        status = "never_succeeded"
    elif staleness > config.freshness_sla_seconds:
        status = "stale"
    elif content_dead:
        status = "dead_feed"
    elif volume_dropped:
        status = "volume_drop"
    elif docs_ingested < config.min_docs_per_window:
        status = "thin"
    elif gap_reason is not None:
        status = "gapped"
    else:
        status = "ok"

    return SourceHealth(
        source_id=config.source_id,
        docs_ingested=docs_ingested,
        expected_min=config.min_docs_per_window,
        last_success_at=last_success_at,
        staleness_seconds=staleness,
        status=status,
        gap_reason=gap_reason,
        content_staleness_seconds=content_staleness,
        baseline_docs=baseline_docs,
    )


@dataclass
class RunHealth:
    """Everything the brief footer reports about the run that produced it."""

    sources: list[SourceHealth] = field(default_factory=list)
    articles_in: int = 0
    clusters_out: int = 0
    exact_duplicates_removed: int = 0
    cache_hit_rate: float = 0.0
    runtime_seconds: float = 0.0
    bytes_scanned: int = 0
    estimated_cost_usd: float = 0.0
    llm_enriched: int = 0
    llm_schema_failures: int = 0

    @property
    def dedup_ratio(self) -> float:
        return self.articles_in / self.clusters_out if self.clusters_out else 0.0

    @property
    def status(self) -> str:
        # `gapped` counts as degraded: an interval nothing can recover is a worse fact
        # about a run than a thin one, and the footer should not read "ok" over it.
        # `dead_feed` and `volume_drop` likewise — a footer reading "ok" over a source
        # that has silently stopped producing is the exact failure §11 exists to prevent.
        if any(s.status in DEGRADED_STATUSES for s in self.sources):
            return "degraded"
        if any(s.status == "thin" for s in self.sources):
            return "thin"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "articles_in": self.articles_in,
            "clusters_out": self.clusters_out,
            "dedup_ratio": round(self.dedup_ratio, 2),
            "exact_duplicates_removed": self.exact_duplicates_removed,
            "cache_hit_rate": round(self.cache_hit_rate, 3),
            "runtime_seconds": round(self.runtime_seconds, 2),
            "estimated_cost_usd": round(self.estimated_cost_usd, 4),
            "sources": [
                {
                    "source_id": s.source_id,
                    "docs": s.docs_ingested,
                    "status": s.status,
                    "staleness_seconds": (
                        None if s.staleness_seconds == float("inf") else round(s.staleness_seconds)
                    ),
                    "content_staleness_seconds": (
                        None
                        if s.content_staleness_seconds is None
                        else round(s.content_staleness_seconds)
                    ),
                    "gap_reason": s.gap_reason,
                }
                for s in self.sources
            ],
        }
