"""`project` cost-allocation tag, read back. 4A.E; SPEC §10.3, §17."""

from __future__ import annotations

from datetime import date

import pytest

from signal_core.ops.costs import METRIC, TAG_KEY, ProjectCost, format_cost, project_cost


class _FakeCostExplorer:
    def __init__(self, periods: list[dict] | None = None) -> None:
        self.periods = periods if periods is not None else []
        self.calls: list[dict] = []

    def get_cost_and_usage(self, **kwargs):
        self.calls.append(kwargs)
        return {"ResultsByTime": self.periods}


def _period(**services: float) -> dict:
    return {
        "Groups": [
            {"Keys": [name], "Metrics": {METRIC: {"Amount": str(amount), "Unit": "USD"}}}
            for name, amount in services.items()
        ]
    }


def test_costs_are_summed_by_service():
    client = _FakeCostExplorer([_period(**{"AWS Lambda": 0.12, "Amazon S3": 0.34})])
    cost = project_cost(date(2026, 7, 23), date(2026, 8, 22), client=client)

    assert cost.total_usd == pytest.approx(0.46)
    assert cost.by_service["Amazon S3"] == pytest.approx(0.34)
    assert cost.top(1) == [("Amazon S3", pytest.approx(0.34))]


def test_a_range_crossing_a_month_boundary_sums_both_periods():
    """MONTHLY granularity returns one entry per month. Reporting only the last would
    silently halve a 30-day figure."""
    client = _FakeCostExplorer([_period(**{"AWS Lambda": 0.10}), _period(**{"AWS Lambda": 0.05})])
    cost = project_cost(date(2026, 7, 23), date(2026, 8, 22), client=client)

    assert cost.total_usd == pytest.approx(0.15)


def test_the_query_filters_on_the_project_tag():
    """The whole point of the carried-forward item: the tag exists, and this is what reads
    it. An unfiltered query would report the whole account, which is a different claim."""
    client = _FakeCostExplorer()
    project_cost(date(2026, 8, 1), date(2026, 8, 22), project="signal", client=client)

    sent = client.calls[0]
    assert sent["Filter"] == {"Tags": {"Key": TAG_KEY, "Values": ["signal"]}}
    assert sent["TimePeriod"] == {"Start": "2026-08-01", "End": "2026-08-22"}
    assert sent["GroupBy"] == [{"Type": "DIMENSION", "Key": "SERVICE"}]


def test_zero_cost_is_the_free_tier_claim_holding():
    """SPEC §10's claim is that this runs inside the always-free tier, and being able to
    state that rather than assume it is the point of reading the tag at all."""
    cost = project_cost(date(2026, 8, 1), date(2026, 8, 22), client=_FakeCostExplorer())
    assert cost.total_usd == 0.0
    assert cost.is_free


def test_an_inverted_range_is_rejected():
    with pytest.raises(ValueError, match="must be before"):
        project_cost(date(2026, 8, 22), date(2026, 8, 1), client=_FakeCostExplorer())


def test_an_empty_result_explains_itself_rather_than_reading_as_free():
    """Cost-allocation tags take up to 24h to appear and never apply retroactively, so an
    empty result has a mundane explanation that is worth printing — otherwise it reads as
    "this is free", which is a claim SPEC §17 would not let stand unexamined."""
    text = format_cost(ProjectCost(start=date(2026, 8, 1), end=date(2026, 8, 22)))
    assert "no tagged costs returned" in text
    assert "retroactively" in text


def test_the_formatted_block_names_the_period_and_its_exclusivity():
    """A report "through the 22nd" that includes the 22nd is a different number from one
    that does not, and two runs disagreeing for an invisible reason is the failure worth
    avoiding."""
    cost = ProjectCost(
        start=date(2026, 8, 1), end=date(2026, 8, 22), total_usd=1.5, by_service={"S3": 1.5}
    )
    text = format_cost(cost)
    assert "2026-08-01 .. 2026-08-22 (end exclusive)" in text
    assert "1.50 USD" in text
