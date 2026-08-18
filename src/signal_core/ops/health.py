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


@dataclass
class SourceHealth:
    source_id: str
    docs_ingested: int
    expected_min: int
    last_success_at: datetime | None
    staleness_seconds: float
    status: str
    gap_reason: str | None = None


def assess_source(
    config: SourceConfig,
    docs_ingested: int,
    last_success_at: datetime | None,
    now: datetime | None = None,
    gap_reason: str | None = None,
) -> SourceHealth:
    """Classify one source.

    `stale` is the important state and the reason this is not just a status-code check:
    the common failure is a feed returning 200 with content that has not moved, not a
    500 (SPEC §11). A source can be succeeding and still be dead.

    `gap_reason` comes from a catch-up run (`ops.recovery`) and means something different
    again: the source is healthy *now*, and an interval is permanently missing anyway.
    SPEC §6.3 requires that be visible rather than implied by a thin day.
    """
    now = now or utc_now()
    staleness = (
        (now - ensure_utc(last_success_at)).total_seconds() if last_success_at else float("inf")
    )

    if last_success_at is None:
        status = "never_succeeded"
    elif staleness > config.freshness_sla_seconds:
        status = "stale"
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
        if any(s.status in {"stale", "never_succeeded", "gapped"} for s in self.sources):
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
                    "gap_reason": s.gap_reason,
                }
                for s in self.sources
            ],
        }
