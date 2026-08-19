"""Ad-hoc and serving Athena queries, with the numbers SPEC §17 promises are real.

Athena bills $5 per TB scanned, with a **10 MB minimum per query** — a `SELECT 1` and a
`SELECT * FROM bronze.raw_documents` both cost at least that. `infra/terraform/main/
query.tf`'s `bytes_scanned_cutoff_per_query` is the guardrail that stops the second one
from being a bad idea; `athena_cost_usd`'s floor here is what stops the *reported* dollar
figure from quietly ignoring the same minimum and printing a number the actual AWS bill
will not match (SPEC §17: never claim a metric the pipeline cannot recompute).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

# Documented Athena minimums/pricing (SPEC §5, §10.3), not derived from a bill:
# re-verify both against AWS's pricing page if this module is more than a few months old.
ATHENA_MIN_BYTES_BILLED = 10 * 1024 * 1024  # 10 MB
ATHENA_USD_PER_TB = 5.0

_TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


def athena_cost_usd(bytes_scanned: int) -> float:
    """What Athena would actually bill for `bytes_scanned`, floored at the 10 MB minimum."""
    billed_bytes = max(bytes_scanned, ATHENA_MIN_BYTES_BILLED)
    return billed_bytes / (1024**4) * ATHENA_USD_PER_TB


@dataclass(frozen=True)
class QueryResult:
    """One completed query: rows plus the numbers `ops.pipeline_costs` records."""

    rows: list[dict[str, str | None]]
    bytes_scanned: int
    engine_execution_ms: int
    cost_usd: float


class AthenaQueryFailed(RuntimeError):
    """The query reached a terminal state other than SUCCEEDED."""


def run_query(
    sql: str,
    *,
    database: str = "silver",
    workgroup: str = "signal",
    client: Any | None = None,
    poll_interval_seconds: float = 1.0,
    timeout_seconds: float = 60.0,
) -> QueryResult:
    """Run `sql` against `database` on `workgroup`, blocking until it finishes.

    `client` is injectable so tests run against `moto` (or a hand-built fake for states
    moto's control-plane-only simulation can't produce, like `FAILED`) instead of real
    AWS — same convention as `state_store.DynamoDBStateStore`.
    """
    athena = client or _athena_client()
    execution_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )["QueryExecutionId"]

    execution = _await_completion(
        athena,
        execution_id,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )

    state = execution["Status"]["State"]
    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "no reason given")
        raise AthenaQueryFailed(f"Athena query {execution_id} {state}: {reason}")

    stats = execution.get("Statistics", {})
    bytes_scanned = int(stats.get("DataScannedInBytes", 0))

    return QueryResult(
        rows=_collect_rows(athena, execution_id),
        bytes_scanned=bytes_scanned,
        engine_execution_ms=int(stats.get("EngineExecutionTimeInMillis", 0)),
        cost_usd=athena_cost_usd(bytes_scanned),
    )


def _await_completion(
    athena: Any,
    execution_id: str,
    *,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    while True:
        execution = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        if execution["Status"]["State"] in _TERMINAL_STATES:
            return execution
        if time.monotonic() - started > timeout_seconds:
            raise TimeoutError(
                f"Athena query {execution_id} still "
                f"{execution['Status']['State']} after {timeout_seconds}s"
            )
        time.sleep(poll_interval_seconds)


def _collect_rows(athena: Any, execution_id: str) -> list[dict[str, str | None]]:
    """`GetQueryResults`, paginated. Athena's first row is always the column header, not
    data — including on a zero-row result, where it's the only row returned."""
    rows: list[dict[str, str | None]] = []
    header: list[str] | None = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"]:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values, strict=True)))
    return rows


def _athena_client() -> Any:
    """boto3 is imported lazily, matching `staging._s3`: local-only callers (and every
    test that injects a client) never need it."""
    import boto3

    return boto3.client("athena")
