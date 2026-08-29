"""The consecutive-brief count. SPEC §16.5; docs/runbooks/phase-5.md 5.A.

The number this computes is the one §16.5 asks the README for, and it had been asserted from
memory until this phase. `compute_streak` is pure and takes its `as_of`, so every case below
is a fixed-date assertion rather than a test that passes until tomorrow.
"""

from __future__ import annotations

from datetime import date

from signal_core.ops.streak import compute_streak

AUG = 2026, 8


def days(*nums: int) -> list[date]:
    return [date(AUG[0], AUG[1], n) for n in nums]


def test_no_briefs_is_not_a_streak_of_zero_dressed_as_one():
    streak = compute_streak([], date(2026, 8, 29))
    assert streak.current == 0
    assert streak.last_date is None
    assert streak.is_live is False
    assert streak.describe() == "no briefs recorded"


def test_a_contiguous_run_counts_every_day():
    streak = compute_streak(days(23, 24, 25, 26, 27, 28, 29), date(2026, 8, 29))
    assert (streak.current, streak.longest, streak.total_briefs) == (7, 7, 7)
    assert streak.missing == ()
    assert streak.first_date == date(2026, 8, 23)
    assert streak.started == date(2026, 8, 23)


def test_the_real_history_on_2026_08_29():
    """The case that motivated the module.

    `docs/runbooks/phase-5.md` opened claiming the streak stood at 3 — "08-23, 08-24, 08-25".
    By the time 5.A ran it was still 3, but a *different* three days, because 08-26 has no
    brief and nothing noticed. A hand-maintained streak drifts in exactly one direction.
    """
    streak = compute_streak(days(23, 24, 25, 27, 28, 29), date(2026, 8, 29))
    assert streak.current == 3
    assert streak.started == date(2026, 8, 27)
    assert streak.first_date == date(2026, 8, 23)
    assert streak.missing == (date(2026, 8, 26),)
    assert streak.total_briefs == 6
    assert streak.is_live is True


def test_a_gap_of_several_days_names_each_one():
    streak = compute_streak(days(20, 25, 26), date(2026, 8, 26))
    assert streak.missing == (
        date(2026, 8, 21),
        date(2026, 8, 22),
        date(2026, 8, 23),
        date(2026, 8, 24),
    )
    assert streak.current == 2


def test_the_longest_run_is_remembered_after_it_breaks():
    """`current` and `longest` answer different questions and must not be collapsed."""
    streak = compute_streak(days(1, 2, 3, 4, 5, 20, 21), date(2026, 8, 21))
    assert streak.current == 2
    assert streak.longest == 5


def test_a_streak_that_ended_is_reported_as_ended_not_as_shorter():
    streak = compute_streak(days(10, 11, 12), date(2026, 8, 29))
    assert streak.current == 3
    assert streak.is_live is False
    assert streak.describe() == "3 days (streak ended 2026-08-12)"


def test_yesterdays_brief_is_still_a_live_streak():
    """The brief DAG fires at 16:00, so for most of any day the newest brief is yesterday's."""
    assert compute_streak(days(27, 28), date(2026, 8, 29)).is_live is True
    assert compute_streak(days(26, 27), date(2026, 8, 29)).is_live is False


def test_duplicate_dates_count_once():
    """`brief_date` is read with DISTINCT, but the counting must not depend on that."""
    streak = compute_streak(days(27, 27, 28, 29), date(2026, 8, 29))
    assert streak.current == 3
    assert streak.total_briefs == 3
