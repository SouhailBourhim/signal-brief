"""What the monitoring DAG decides, without Airflow or Spark. SPEC §11, §6.3.

The DAG's tasks are plumbing — build a session, count rows, write a table. The judgement
is here, so it can be tested against the cases that matter: a source that is fine, one
that is quietly dead, and one that came back but lost the interval it was away for.
"""

from __future__ import annotations

from datetime import timedelta

from signal_core.config import SOURCES
from signal_core.contracts import State
from signal_core.ops.monitor import assess, window_bounds
from signal_core.timeutil import utc_now


def test_healthy_source_needs_nothing():
    now = utc_now()
    state = State(source_id="rss_tech", last_success_at=now - timedelta(minutes=5))

    verdict = assess(SOURCES["rss_tech"], state, docs_in_window=8, now=now)

    assert verdict.health.status == "ok"
    assert verdict.catch_up is None
    assert not verdict.needs_catch_up


def test_source_past_its_sla_is_stale_and_gets_a_plan():
    """rss_tech's SLA is 30 minutes. Three hours of silence is an outage, and the plan
    that follows says most of it is unrecoverable — the feed window is an hour."""
    now = utc_now()
    state = State(source_id="rss_tech", last_success_at=now - timedelta(hours=3))

    verdict = assess(SOURCES["rss_tech"], state, docs_in_window=0, now=now)

    assert verdict.health.status == "stale"
    assert verdict.catch_up is not None
    assert verdict.catch_up.gap_reason is not None
    assert verdict.catch_up.gap_seconds > 3600


def test_complete_horizon_source_plans_a_full_recovery():
    now = utc_now()
    state = State(source_id="hackernews", last_success_at=now - timedelta(hours=6))

    verdict = assess(SOURCES["hackernews"], state, docs_in_window=0, now=now)

    assert verdict.needs_catch_up
    assert verdict.catch_up.is_complete  # nothing lost, just late
    assert verdict.health.gap_reason is None


def test_a_source_that_never_ran_is_not_a_gap():
    """No `last_success_at` means never started, not "was down and lost data". Planning
    catch-up from the beginning of time would invent an outage and a gap to match."""
    now = utc_now()

    verdict = assess(SOURCES["edgar"], State(source_id="edgar"), docs_in_window=0, now=now)

    assert verdict.health.status == "never_succeeded"
    assert verdict.catch_up is None


def test_window_is_the_hour_that_just_closed():
    """Assessing a partial current hour makes every run look thin at :01, which is how a
    genuine volume alert gets trained away."""
    now = utc_now().replace(minute=37)
    start, end = window_bounds(now)

    assert end <= now
    assert end.minute == 0 and end.second == 0
    assert end - start == timedelta(hours=1)
