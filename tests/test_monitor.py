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


# --- SPEC §11's two failure modes, neither of which was detectable before 2026-08-20 ---


def test_a_frozen_feed_returning_200s_is_dead_not_ok():
    """The failure SPEC §11 calls the common one: content stops moving, fetches keep
    succeeding. `last_success_at` cannot see it — it advances on a 304 and on an
    unchanged 200 alike — so before `last_content_change_at` this source read `ok`
    indefinitely."""
    now = utc_now()
    state = State(
        source_id="rss_tech",
        last_success_at=now - timedelta(minutes=1),  # fetching fine, right now
        last_content_change_at=now - timedelta(days=5),  # nothing new in five days
    )

    verdict = assess(SOURCES["rss_tech"], state, docs_in_window=0, now=now)

    assert verdict.health.status == "dead_feed"
    # The fetch itself is healthy, and the verdict must not misreport that as an outage:
    # this is a live poller against a dead publisher, not a broken poller.
    assert verdict.health.staleness_seconds < SOURCES["rss_tech"].freshness_sla_seconds
    assert verdict.catch_up is None


def test_a_quiet_rss_hour_is_not_a_dead_feed():
    """The other side of the same check. An RSS feed that 304s all hour and published
    nothing this morning is completely healthy; firing here is what trains the alert
    away (SPEC §11) and is why the content SLA is days, not minutes."""
    now = utc_now()
    state = State(
        source_id="rss_ars",
        last_success_at=now - timedelta(minutes=2),
        last_content_change_at=now - timedelta(hours=6),
    )

    verdict = assess(SOURCES["rss_ars"], state, docs_in_window=0, now=now)

    assert verdict.health.status == "ok"


def test_an_80_percent_volume_drop_alerts():
    """SPEC §11, verbatim: "A source dropping 80% overnight should alert, not silently
    thin the brief." Hacker News stays well above its static floor here — only the
    comparison against its own baseline catches this."""
    now = utc_now()
    state = State(
        source_id="hackernews",
        last_success_at=now - timedelta(minutes=1),
        last_content_change_at=now - timedelta(minutes=1),
    )

    verdict = assess(SOURCES["hackernews"], state, docs_in_window=100, now=now, baseline_docs=800.0)

    assert verdict.health.status == "volume_drop"
    assert verdict.health.docs_ingested > SOURCES["hackernews"].min_docs_per_window


def test_a_drop_to_exactly_the_threshold_alerts():
    """The boundary §11's own wording lands on: 900/hour falling to 180 *is* a drop of
    80%. A strict `<` read this as ok, which is the one figure most likely to be the
    real one."""
    now = utc_now()
    state = State(
        source_id="hackernews",
        last_success_at=now - timedelta(minutes=1),
        last_content_change_at=now - timedelta(minutes=1),
    )

    verdict = assess(SOURCES["hackernews"], state, docs_in_window=180, now=now, baseline_docs=900.0)

    assert verdict.health.status == "volume_drop"


def test_normal_variation_against_baseline_is_not_a_drop():
    """A slow hour is not an incident. 60% of baseline is well inside a diurnal cycle."""
    now = utc_now()
    state = State(
        source_id="hackernews",
        last_success_at=now - timedelta(minutes=1),
        last_content_change_at=now - timedelta(minutes=1),
    )

    verdict = assess(SOURCES["hackernews"], state, docs_in_window=480, now=now, baseline_docs=800.0)

    assert verdict.health.status == "ok"


def test_low_volume_sources_are_not_judged_on_percentages():
    """`rss_ars` averages well under one document an hour, so "dropped 80%" is a
    statement about noise. It reports ok and is covered by the dead-feed check instead."""
    now = utc_now()
    state = State(
        source_id="rss_ars",
        last_success_at=now - timedelta(minutes=1),
        last_content_change_at=now - timedelta(hours=1),
    )

    verdict = assess(SOURCES["rss_ars"], state, docs_in_window=0, now=now, baseline_docs=1.0)

    assert verdict.health.status == "ok"


def test_hackernews_going_silent_for_an_hour_is_not_ok():
    """The regression this whole change exists for. `min_docs_per_window` was 0 for the
    pipeline's highest-volume source — commented for a one-minute window while the
    assessment window is an hour — so a totally silent Hacker News reported `ok`."""
    now = utc_now()
    state = State(
        source_id="hackernews",
        last_success_at=now - timedelta(minutes=1),
        last_content_change_at=now - timedelta(minutes=1),
    )

    verdict = assess(SOURCES["hackernews"], state, docs_in_window=0, now=now)

    assert verdict.health.status == "thin"


def test_every_non_ok_status_fails_the_dag():
    """`thin` used to be a status `assess_source` could return and the DAG had never
    heard of, so a source producing zero documents ran green. The sets are derived from
    one definition now; this asserts nothing can be added to one without the other."""
    from signal_core.ops.health import FAILING_STATUSES

    producible = {"never_succeeded", "stale", "dead_feed", "volume_drop", "thin", "gapped"}

    assert producible == set(FAILING_STATUSES)
