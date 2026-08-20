"""Turning a window of bronze into a health verdict per source. SPEC §11, §6.3.

Kept free of Spark and Airflow so the interesting part — what counts as unhealthy — is
unit-testable without either. The DAG supplies the counts and the state; this decides
what they mean, and whether an outage large enough to need catch-up has occurred.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from signal_core.contracts import SourceConfig, State
from signal_core.ops.health import SourceHealth, assess_source
from signal_core.ops.recovery import CatchUpPlan, plan_catch_up
from signal_core.timeutil import ensure_utc, utc_now


@dataclass(frozen=True)
class SourceVerdict:
    """One source's health, plus the recovery plan if it needs one."""

    health: SourceHealth
    catch_up: CatchUpPlan | None

    @property
    def needs_catch_up(self) -> bool:
        return self.catch_up is not None and self.catch_up.has_work


def assess(
    config: SourceConfig,
    state: State,
    docs_in_window: int,
    *,
    window_start: datetime | None = None,
    now: datetime | None = None,
    baseline_docs: float | None = None,
) -> SourceVerdict:
    """Assess one source over one window, planning catch-up if it has been down.

    An outage is defined by `last_success_at`, not by a missing schedule run: EventBridge
    firing on time while every fetch fails is exactly as much of an outage as the
    schedule being disabled, and only the watermark can tell the difference.

    `baseline_docs` is this source's own recent typical output, supplied by the caller
    (`ingest_monitor` reads it from `ops.source_health`) rather than computed here, for
    the same reason the document count is: this module stays free of Spark so the
    interesting part — what counts as unhealthy — is unit-testable without a JVM.
    """
    now = ensure_utc(now or utc_now())
    last_success = ensure_utc(state.last_success_at) if state.last_success_at else None
    last_content_change = (
        ensure_utc(state.last_content_change_at) if state.last_content_change_at else None
    )

    catch_up: CatchUpPlan | None = None
    if last_success is not None and (now - last_success).total_seconds() > (
        config.freshness_sla_seconds
    ):
        # The outage ran from the last success to now. Handing the whole span to
        # plan_catch_up — rather than only the part inside the horizon — is what makes
        # the unrecoverable remainder appear instead of being silently trimmed away.
        catch_up = plan_catch_up(config, last_success, now, now=now)

    health = assess_source(
        config,
        docs_ingested=docs_in_window,
        last_success_at=last_success,
        now=now,
        gap_reason=catch_up.gap_reason if catch_up else None,
        last_content_change_at=last_content_change,
        baseline_docs=baseline_docs,
    )
    # `window_start` is carried by the caller into ops.source_health; nothing here needs
    # it, and inventing a default would put a wrong timestamp in a history table.
    del window_start
    return SourceVerdict(health=health, catch_up=catch_up)


def window_bounds(now: datetime | None = None, *, hours: int = 1) -> tuple[datetime, datetime]:
    """The closed hour before `now`. Monitoring a partial current hour makes every run
    look thin in its first minutes, which is how a real alert gets trained away."""
    now = ensure_utc(now or utc_now())
    end = now.replace(minute=0, second=0, microsecond=0)
    return end - timedelta(hours=hours), end
