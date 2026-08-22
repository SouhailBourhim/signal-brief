"""Parity between the places a source has to be declared. SPEC §3, §6.1; docs/runbooks/
phase-2.md 2.B.

SPEC §3 makes adding source #6 a 30-minute job, and §6.1 says the whole cost is "write the
module, add one entry here, add one Terraform map entry". That claim is only true while
`SOURCES`, `sources.REGISTRY`, `parse.REGISTRY`, and Terraform's `var.sources` all agree,
and the failure mode is quiet in every direction: a source in `SOURCES` but not
`sources.REGISTRY` raises only when its Lambda runs, one missing from `parse.REGISTRY`
raises only when `normalize_window` runs, and one missing from Terraform simply never gets
scheduled — which looks exactly like a healthy source nobody is polling.

So the parity is asserted rather than remembered. These tests are the reason the claim can
be made in the README without a caveat.
"""

from __future__ import annotations

import itertools
import re
from pathlib import Path

import pytest

from signal_core.config import DEPLOYED_SOURCE_IDS, SOURCES
from signal_core.parse import REGISTRY as PARSE_REGISTRY
from signal_core.sources import REGISTRY

TERRAFORM_MAIN = Path(__file__).resolve().parents[1] / "infra/terraform/main/main.tf"


def _terraform_sources() -> dict[str, str]:
    """`{source_id: schedule_expression}` from `variable "sources"`'s default block.

    Parsed with a regex rather than a HCL library on purpose: adding a parser dependency to
    test one map is a worse trade than a brittle read of a file that is 12 lines of
    formatting away from failing loudly. `terraform fmt -check` in CI is what keeps the
    indentation this relies on stable.
    """
    text = TERRAFORM_MAIN.read_text(encoding="utf-8")
    start = text.index('variable "sources"')
    default = text.index("default = {", start)

    depth, end = 0, None
    for index in range(default, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                end = index
                break
    assert end is not None, 'unbalanced braces in variable "sources"'
    block = text[default:end]

    entries = re.findall(r"^    (\w+) = \{(.*?)^    \}", block, re.MULTILINE | re.DOTALL)
    out = {}
    for name, body in entries:
        schedule = re.search(r'schedule_expression\s*=\s*"([^"]+)"', body)
        assert schedule, f"{name} has no schedule_expression"
        out[name] = schedule.group(1)
    return out


_RATE = re.compile(r"rate\((\d+) minutes?\)")
_CRON_HOURLY = re.compile(r"cron\(([\d,]+) \*.*\)")
_CRON_DAILY = re.compile(r"cron\((\d+) (\d+) .*\)")


def _cadence_seconds(expression: str) -> int | None:
    """The longest gap between two consecutive fires, in seconds, or None if unparsed.

    `rate(N minutes)` is the whole story for the Phase 1-2 sources. 4A added two cron
    schedules — `hn_scores` because `rate(...)` gives no control over *phase* and that source
    exists to not collide with the others, `market` because it runs once a day at a chosen
    hour. The longest gap is the right reading for all three: it is the interval an SLA has
    to survive, and for a minute list it is the one wrapping across the hour boundary rather
    than any sitting inside it.
    """
    if rate := _RATE.fullmatch(expression):
        return int(rate.group(1)) * 60

    if hourly := _CRON_HOURLY.fullmatch(expression):
        fires = sorted(int(m) for m in hourly.group(1).split(","))
        gaps = [b - a for a, b in itertools.pairwise(fires)]
        gaps.append(fires[0] + 60 - fires[-1])  # wrapping into the next hour
        return max(gaps) * 60

    if _CRON_DAILY.fullmatch(expression):
        return 24 * 60 * 60
    return None


def _fire_minutes(expression: str) -> set[int]:
    """Which minutes past the hour a schedule can fire at.

    Concurrency is a same-minute question, so this deliberately ignores *which* hour a daily
    schedule lands in: two schedules that share a minute collide on the days they coincide,
    and a schedule that shares no minute with anything never collides at all. The stronger
    property is the one worth holding.
    """
    if rate := _RATE.fullmatch(expression):
        # `rate` has no guaranteed phase, so the conservative reading is that it can land
        # on any multiple of its step — including 0.
        return set(range(0, 60, int(rate.group(1))))
    if hourly := _CRON_HOURLY.fullmatch(expression):
        return {int(m) for m in hourly.group(1).split(",")}
    if daily := _CRON_DAILY.fullmatch(expression):
        return {int(daily.group(1))}
    raise AssertionError(f"unhandled schedule expression {expression!r}")


def test_every_configured_source_has_a_poller():
    """A source in SOURCES without a REGISTRY entry fails only when its Lambda runs."""
    assert set(SOURCES) == set(REGISTRY)


def test_every_configured_source_has_a_parser():
    """docs/runbooks/phase-2.md 2.B: `parse.REGISTRY` mirrors `sources.REGISTRY`
    specifically so the 30-minute claim stays true on the silver side too. A source
    wired for ingestion but missing here would poll and commit to bronze successfully,
    then fail only when `normalize_window` calls `get_parser` — the exact "fails only
    when it runs" failure mode this whole file exists to catch on the ingest side."""
    assert set(SOURCES) == set(PARSE_REGISTRY)


def test_every_deployed_source_is_scheduled_in_terraform():
    """...and one missing from Terraform is never polled, which looks like a quiet source."""
    assert set(DEPLOYED_SOURCE_IDS) == set(_terraform_sources())


def test_fake_is_not_deployed():
    """The Phase 0 fixture source has no Lambda, schedule, or state item. Assessing it
    would report a permanent outage for something that was never running."""
    assert "fake" in SOURCES
    assert "fake" not in DEPLOYED_SOURCE_IDS
    assert "fake" not in _terraform_sources()


def test_the_deployed_source_count_is_what_the_phases_claim():
    """SPEC §3: three in Phase 1, six by Phase 2 — the sixth being the one §3's "adding
    source #6 must be a 30-minute job" is measured against. Phase 4A adds `hn_scores` for
    §7.4's velocity component, which SPEC §12 carried forward from Phase 2, and `market`
    for its market-corroboration component.

    A literal, deliberately: this is the only assertion that notices a source being added
    or dropped without anyone deciding to, and comparing the config against itself would
    notice nothing."""
    assert len(DEPLOYED_SOURCE_IDS) == 8


@pytest.mark.parametrize("source_id", ["hn_scores", "market"])
def test_the_phase_4a_pollers_do_not_collide_with_the_phase_1_2_six(source_id: str):
    """Why both 4A sources are scheduled with cron. SPEC §3's map comment records that six
    pollers fit under a new account's total concurrency limit of 10 only because they fit,
    and that "source #7 is where it stops fitting". Sources #7 and #8 exist now.

    They fit by never firing at the same minute as anything else. `rate(N minutes)` cannot
    express that — it has no phase — so this asserts the property the cron expressions were
    chosen for, rather than the expressions themselves, which are free to change as long as
    the property holds."""
    schedules = _terraform_sources()
    mine = _fire_minutes(schedules[source_id])
    for other_id, expression in schedules.items():
        if other_id == source_id:
            continue
        overlap = mine & _fire_minutes(expression)
        assert not overlap, (
            f"{source_id} fires at the same minute as {other_id} ({sorted(overlap)}), "
            "raising the concurrency peak — see the sources map comment"
        )


@pytest.mark.parametrize("source_id", sorted(DEPLOYED_SOURCE_IDS))
def test_freshness_sla_is_longer_than_the_poll_cadence(source_id: str):
    """The inversion that trains an alert away, caught at build time. SPEC §11.

    An SLA shorter than the interval between polls reports a perfectly healthy source as
    permanently stale, so the alert gets ignored and then deleted — arriving at §11's
    stale-feed blindness from the opposite direction. The deployed convention is 3x the
    cadence; 2x is asserted so the bound catches the bug without breaking on a tuning
    change. Phase 1's runbook records why this is a real mistake and not a theoretical one.
    """
    expression = _terraform_sources()[source_id]
    cadence_seconds = _cadence_seconds(expression)
    assert cadence_seconds, f"{source_id}: unhandled schedule expression {expression!r}"

    assert SOURCES[source_id].freshness_sla_seconds >= 2 * cadence_seconds, (
        f"{source_id}: SLA {SOURCES[source_id].freshness_sla_seconds}s is too tight for a "
        f"{cadence_seconds}s cadence — it would report a healthy source as stale"
    )
