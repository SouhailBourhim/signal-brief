"""What this project actually costs, by cost-allocation tag. SPEC §10.3, §17.

## The gap this closes

The `project` tag has been on every taggable resource since Phase 1, and it was activated as
a cost-allocation tag on 2026-08-20 (`docs/runbooks/phase-1.md` 1.B). SPEC §12 carried it into
4A anyway, and the carried item was right — but not for the reason it was written down. The
*tagging* was done; what was missing was anything that reads it back.

`ops/athena.py::athena_cost_usd` is a different number. It is Athena's own byte-scanned
estimate for one query, computed locally from a published rate — useful, exact for its
purpose, and completely blind to Lambda invocations, S3 storage and requests, DynamoDB, data
transfer, and every other line on the bill. SPEC §10.3 asks what the *project* costs. Only
Cost Explorer can answer that, and nothing here called it.

## Deliberately not in a DAG

Cost Explorer data lags up to 24 hours and is restated as AWS finalizes charges. A number
that is stale by construction has no business in a daily pipeline task presented as a live
metric — SPEC §17's rule is that a metric this pipeline cannot recompute does not get claimed.
So this is a manual verb, run when the README's cost figure needs refreshing, in the same
spirit as `make dictionary`.

That also keeps it free: `GetCostAndUsage` bills $0.01 per request, which is nothing once a
week and real money on an hourly schedule.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

TAG_KEY = "project"
DEFAULT_PROJECT = "signal"

# What Cost Explorer calls the metric. `UnblendedCost` is the actual charge for the account,
# as opposed to `AmortizedCost`, which spreads reservation costs — irrelevant on a free-tier
# account with no reservations, but the distinction matters if this ever grows one.
METRIC = "UnblendedCost"


@dataclass(frozen=True)
class ProjectCost:
    """What the project cost over a period, split by service."""

    start: date
    end: date
    total_usd: float = 0.0
    by_service: dict[str, float] = field(default_factory=dict)
    currency: str = "USD"

    @property
    def is_free(self) -> bool:
        """SPEC §10's claim is that this runs inside the always-free tier. A total of zero
        is that claim holding, and it is worth being able to state rather than assume."""
        return self.total_usd == 0.0

    def top(self, n: int = 5) -> list[tuple[str, float]]:
        return sorted(self.by_service.items(), key=lambda kv: -kv[1])[:n]


def _ce_client() -> Any:
    """Lazily, matching `ops/athena.py::_athena_client`.

    Cost Explorer is a global service whose endpoint lives in us-east-1 regardless of where
    the resources are, so the region is pinned rather than taken from settings — a caller
    with `AWS_REGION=eu-west-1` would otherwise get an endpoint that does not exist.
    """
    import boto3

    return boto3.client("ce", region_name="us-east-1")


def project_cost(
    start: date | None = None,
    end: date | None = None,
    *,
    project: str = DEFAULT_PROJECT,
    client: Any | None = None,
) -> ProjectCost:
    """Cost for one `project` tag value over `[start, end)`.

    Defaults to the last 30 days. The end is exclusive, which is Cost Explorer's own
    convention and worth not papering over — a report "through the 22nd" that includes the
    22nd is a different number from one that does not, and quietly picking one would make
    two runs of this disagree for a reason nobody could see.
    """
    end = end or date.today()
    start = start or (end - timedelta(days=30))
    if start >= end:
        raise ValueError(f"start {start} must be before end {end}")

    response = (client or _ce_client()).get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="MONTHLY",
        Metrics=[METRIC],
        Filter={"Tags": {"Key": TAG_KEY, "Values": [project]}},
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )

    by_service: dict[str, float] = {}
    currency = "USD"
    for period in response.get("ResultsByTime", []):
        for group in period.get("Groups", []):
            keys = group.get("Keys") or ["unknown"]
            metric = (group.get("Metrics") or {}).get(METRIC, {})
            amount = float(metric.get("Amount", 0.0) or 0.0)
            currency = metric.get("Unit") or currency
            # Summed across periods rather than replaced: MONTHLY granularity over a range
            # that crosses a month boundary returns one entry per month, and reporting only
            # the last would silently halve a 30-day figure.
            by_service[keys[0]] = by_service.get(keys[0], 0.0) + amount

    return ProjectCost(
        start=start,
        end=end,
        total_usd=round(sum(by_service.values()), 6),
        by_service=by_service,
        currency=currency,
    )


def format_cost(cost: ProjectCost) -> str:
    """A block suitable for pasting into the README, which is where SPEC §16 wants it."""
    lines = [
        f"project={DEFAULT_PROJECT}  {cost.start} .. {cost.end} (end exclusive)",
        f"total: {cost.total_usd:.2f} {cost.currency}",
    ]
    if cost.is_free:
        lines.append("  (inside the always-free tier — SPEC §10)")
    for service, amount in cost.top():
        lines.append(f"  {amount:>10.4f}  {service}")
    if not cost.by_service:
        lines.append(
            "  no tagged costs returned — cost-allocation tags take up to 24h to appear, "
            "and only apply from activation forward (never retroactively)"
        )
    return "\n".join(lines)
