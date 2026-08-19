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


def test_phase_2_reaches_six_deployed_sources():
    """SPEC §3: three in Phase 1, and Phase 2 takes it to six — the sixth being the one
    §3's "adding source #6 must be a 30-minute job" is measured against."""
    assert len(DEPLOYED_SOURCE_IDS) == 6


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
    minutes = re.fullmatch(r"rate\((\d+) minutes?\)", expression)
    assert minutes, f"{source_id}: unhandled schedule expression {expression!r}"

    cadence_seconds = int(minutes.group(1)) * 60
    assert SOURCES[source_id].freshness_sla_seconds >= 2 * cadence_seconds, (
        f"{source_id}: SLA {SOURCES[source_id].freshness_sla_seconds}s is too tight for a "
        f"{cadence_seconds}s cadence — it would report a healthy source as stale"
    )
