"""Replay and catch-up. SPEC §6.3, §12.

Two words that the documents this project supersedes used interchangeably, and that make
very different promises:

  * **Replay** reprocesses an interval from bytes already in `bronze/`. It touches no
    network, it is deterministic, and it always works. This is the guarantee the project
    actually makes. `spark/jobs/commit_bronze.py` is the mechanism: a MERGE on
    `ingest_id`, so re-running over an interval already committed inserts nothing.

  * **Catch-up** re-fetches what was missed while the pipeline was down. It is bounded by
    each source's backfill horizon (SPEC §3), and for RSS it is *partial by construction*
    — items published and rotated out of the feed during an outage are gone, and no
    amount of engineering recovers them.

This module is the second one, and specifically the part that is easy to skip: computing
what a source genuinely cannot recover and writing it down as a `gap_reason`, so the loss
appears in `ops.source_health` instead of being implied by a thin day nobody explains.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from signal_core.contracts import BackfillHorizon, SourceConfig
from signal_core.timeutil import ensure_utc, utc_now

# How far back each horizon can actually reach. WINDOW is the honest one: RSS feeds hold
# roughly 1-3 hours of items (SPEC §3), so 1 hour is the conservative floor — claiming
# three and recovering one is worse than claiming one.
HORIZON_REACH: dict[BackfillHorizon, timedelta | None] = {
    BackfillHorizon.COMPLETE: None,  # None means unbounded, not zero
    BackfillHorizon.DAY: timedelta(days=1),
    BackfillHorizon.WINDOW: timedelta(hours=1),
    BackfillHorizon.NONE: timedelta(0),
}


@dataclass(frozen=True)
class CatchUpPlan:
    """What a source can and cannot recover for one outage interval.

    `recoverable_from`/`recoverable_until` is the sub-interval worth re-fetching.
    `gap_start`/`gap_end` is what is permanently missing, and `gap_reason` says why in
    words a reader of the brief's health footer can act on.
    """

    source_id: str
    outage_start: datetime
    outage_end: datetime
    recoverable_from: datetime | None
    recoverable_until: datetime | None
    gap_start: datetime | None
    gap_end: datetime | None
    gap_reason: str | None

    @property
    def is_complete(self) -> bool:
        """True when nothing was lost — the only case where a catch-up run leaves the
        record as good as if the outage had not happened."""
        return self.gap_reason is None

    @property
    def has_work(self) -> bool:
        return self.recoverable_from is not None

    @property
    def gap_seconds(self) -> float:
        if self.gap_start is None or self.gap_end is None:
            return 0.0
        return max(0.0, (self.gap_end - self.gap_start).total_seconds())


def plan_catch_up(
    config: SourceConfig,
    outage_start: datetime,
    outage_end: datetime,
    now: datetime | None = None,
) -> CatchUpPlan:
    """Split an outage into what this source can re-fetch and what it has lost.

    The horizon is measured backwards from *now*, not from the end of the outage: a
    source that was down for an hour but only noticed three days later has a three-day
    reach problem, not a one-hour one. That distinction is the difference between a
    catch-up that quietly recovers nothing and one that says so.
    """
    now = ensure_utc(now or utc_now())
    outage_start = ensure_utc(outage_start)
    outage_end = min(ensure_utc(outage_end), now)

    if outage_end <= outage_start:
        return CatchUpPlan(
            source_id=config.source_id,
            outage_start=outage_start,
            outage_end=outage_end,
            recoverable_from=None,
            recoverable_until=None,
            gap_start=None,
            gap_end=None,
            gap_reason=None,
        )

    reach = HORIZON_REACH[config.backfill_horizon]
    horizon_floor = None if reach is None else now - reach

    if horizon_floor is None or horizon_floor <= outage_start:
        # The whole outage is inside the horizon.
        return CatchUpPlan(
            source_id=config.source_id,
            outage_start=outage_start,
            outage_end=outage_end,
            recoverable_from=outage_start,
            recoverable_until=outage_end,
            gap_start=None,
            gap_end=None,
            gap_reason=None,
        )

    if horizon_floor >= outage_end:
        # The horizon does not reach into the outage at all: nothing is recoverable.
        return CatchUpPlan(
            source_id=config.source_id,
            outage_start=outage_start,
            outage_end=outage_end,
            recoverable_from=None,
            recoverable_until=None,
            gap_start=outage_start,
            gap_end=outage_end,
            gap_reason=_reason(config, outage_start, outage_end),
        )

    return CatchUpPlan(
        source_id=config.source_id,
        outage_start=outage_start,
        outage_end=outage_end,
        recoverable_from=horizon_floor,
        recoverable_until=outage_end,
        gap_start=outage_start,
        gap_end=horizon_floor,
        gap_reason=_reason(config, outage_start, horizon_floor),
    )


def _reason(config: SourceConfig, gap_start: datetime, gap_end: datetime) -> str:
    """A sentence, not a code. This string ends up in the brief's health footer, where
    its reader is a person deciding whether a thin day is a bug or a fact."""
    lost = gap_end - gap_start
    hours = lost.total_seconds() / 3600
    span = f"{hours:.1f}h" if hours < 48 else f"{lost.days}d"
    detail = {
        BackfillHorizon.WINDOW: (
            "the feed holds only its current window, so items published during the outage "
            "have rotated out and are unrecoverable"
        ),
        BackfillHorizon.DAY: (
            "the current-filings feed reaches back about a day; recovering further needs "
            "the daily index, which is not wired up"
        ),
        BackfillHorizon.NONE: "this source cannot be re-fetched at all",
        BackfillHorizon.COMPLETE: "unexpected: a complete-horizon source should not gap",
    }[config.backfill_horizon]
    return f"{span} unrecovered ({config.backfill_horizon.value} horizon): {detail}"
