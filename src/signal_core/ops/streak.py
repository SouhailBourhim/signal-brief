"""How many consecutive days the brief has actually run. SPEC §12, §16.5.

## Why this is computed rather than counted by hand

§16.5 asks the README for "a screenshot of a real morning brief, **and how many consecutive
days it has run**". That second clause is the only number in §16 that is a claim about
operational reliability rather than about a measurement, and it is the one this project had
been asserting from memory. `docs/runbooks/phase-5.md` opened saying the streak "stands at 3
(08-23, 08-24, 08-25)". By the time the section was executed the truth was a *different* three
days — 08-27, 08-28, 08-29 — because **2026-08-26 has no brief at all** and nothing noticed.

A hand-maintained streak has exactly one failure mode and it is the one that matters: it drifts
upward. Nobody forgets to increment it and everybody forgets to reset it. So the number comes
from `gold.brief_items`, which has a row per brief that was actually built, and the gaps come
back with it — a streak that reports its own holes cannot quietly become a longer streak than
it earned.

## What counts as a day

A `brief_date` present in `gold.brief_items`. That is the record of a brief being *built and
recorded*, which is the strongest claim this table can support; whether it was read is 4A's
acceptance and lives in the runbook's daily-read table, not here. `items.py` writes the row at
render time, so a brief that rendered and failed to send still counts — deliberately, because
the send is retried and the artifact exists either way (`make brief-open` still opens it).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from itertools import pairwise

# How stale the newest brief may be before the streak is considered broken rather than
# merely not-yet-run-today. The brief DAG fires at 16:00 (ADR-0010's amendment), so for most
# of any given day the newest brief is legitimately yesterday's.
LIVE_TOLERANCE_DAYS = 1


@dataclass(frozen=True)
class Streak:
    """The streak, its history, and the holes in it."""

    current: int
    longest: int
    last_date: date | None
    # The first brief ever recorded, and the first of the *current* run. They differ exactly
    # when the streak has been broken at least once, which is the case worth being able to see.
    first_date: date | None
    started: date | None
    missing: tuple[date, ...]
    total_briefs: int
    # The moment the streak was computed against, kept so `is_live` is answerable from the
    # value alone rather than needing the caller to remember what it asked about.
    as_of: date | None = None

    @property
    def is_live(self) -> bool:
        """Whether `current` is still running, or is a run that ended in the past.

        Separate from `current` because the two answer different questions and collapsing
        them would make a broken streak read as a shorter one. A streak of 9 that ended a
        week ago is not a streak of 9; it is also not a streak of 0, and the README should be
        able to say which.
        """
        return self._days_since_last is not None and self._days_since_last <= LIVE_TOLERANCE_DAYS

    @property
    def _days_since_last(self) -> int | None:
        if self.last_date is None or self.as_of is None:
            return None
        return (self.as_of - self.last_date).days

    def describe(self) -> str:
        """One line, for the brief's footer and the README."""
        if self.current == 0:
            return "no briefs recorded"
        word = "day" if self.current == 1 else "days"
        if not self.is_live:
            return f"{self.current} {word} (streak ended {self.last_date})"
        return f"day {self.current}"


def compute_streak(brief_dates: Iterable[date], as_of: date) -> Streak:
    """The consecutive-day run ending at the most recent brief, plus what it skipped.

    `as_of` is injected rather than read from the clock for the same reason
    `ranker.score_cluster` takes a `now`: a streak is a statement about a moment, and a test
    that cannot fix the moment cannot assert on the answer.
    """
    days = sorted(set(brief_dates))
    if not days:
        return Streak(0, 0, None, None, None, (), 0, as_of=as_of)

    # One pass: the run ending at each day, tracking the longest seen and the run in progress.
    longest = 1
    run = 1
    run_start = days[0]
    current_start = days[0]
    missing: list[date] = []
    for previous, day in pairwise(days):
        gap = (day - previous).days
        if gap == 1:
            run += 1
        else:
            missing.extend(previous + timedelta(days=n) for n in range(1, gap))
            run = 1
            run_start = day
        longest = max(longest, run)
        current_start = run_start

    return Streak(
        current=run,
        longest=longest,
        last_date=days[-1],
        first_date=days[0],
        started=current_start,
        missing=tuple(missing),
        total_briefs=len(days),
        as_of=as_of,
    )
