"""`ops/athena.py`. SPEC §5, §10.3, §17.

Two layers of fake, deliberately: `moto`'s Athena backend simulates the control plane
(workgroups, query execution state) but not a real query engine — `start_query_execution`
returns `SUCCEEDED` immediately with zero bytes scanned, whatever the SQL says. That's
still worth exercising, because it proves the real boto3 call shapes (pagination
included) are right. Everything that needs a specific state sequence — `FAILED`, a
multi-poll `RUNNING` -> terminal transition, a timeout, real rows with real columns —
uses a small hand-built fake client instead, the way `moto` can't produce them.
"""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from moto import mock_aws

from signal_core.ops.athena import AthenaQueryFailed, athena_cost_usd, run_query


class _FakePaginator:
    def __init__(self, columns: list[str], rows: list[list[str]]) -> None:
        self._columns = columns
        self._rows = rows

    def paginate(self, **kwargs: Any):
        del kwargs
        header = {"Data": [{"VarCharValue": c} for c in self._columns]}
        body = [{"Data": [{"VarCharValue": v} for v in row]} for row in self._rows]
        yield {"ResultSet": {"Rows": [header, *body]}}


class _FakeAthenaClient:
    """Just enough of the boto3 Athena surface for `run_query` to exercise every branch."""

    def __init__(
        self,
        *,
        final_state: str = "SUCCEEDED",
        reason: str | None = None,
        bytes_scanned: int = 0,
        engine_ms: int = 0,
        columns: list[str] | None = None,
        rows: list[list[str]] | None = None,
        polls_before_terminal: int | float = 0,  # `float("inf")` for "never terminal"
    ) -> None:
        self.final_state = final_state
        self.reason = reason
        self.bytes_scanned = bytes_scanned
        self.engine_ms = engine_ms
        self.columns = columns or []
        self.rows = rows or []
        self.polls_before_terminal = polls_before_terminal
        self.poll_count = 0
        self.started_with: dict[str, Any] | None = None

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.started_with = kwargs
        return {"QueryExecutionId": "fake-execution-id"}

    def get_query_execution(self, QueryExecutionId: str) -> dict[str, Any]:
        assert QueryExecutionId == "fake-execution-id"
        self.poll_count += 1
        if self.poll_count <= self.polls_before_terminal:
            return {"QueryExecution": {"Status": {"State": "RUNNING"}}}
        status: dict[str, Any] = {"State": self.final_state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {
            "QueryExecution": {
                "Status": status,
                "Statistics": {
                    "DataScannedInBytes": self.bytes_scanned,
                    "EngineExecutionTimeInMillis": self.engine_ms,
                },
            }
        }

    def get_paginator(self, operation_name: str) -> _FakePaginator:
        assert operation_name == "get_query_results"
        return _FakePaginator(self.columns, self.rows)


# --- athena_cost_usd: the SPEC §17 floor ------------------------------------------------


def test_cost_floors_at_the_ten_mb_minimum():
    assert athena_cost_usd(0) == athena_cost_usd(5 * 1024 * 1024)


def test_cost_scales_linearly_above_the_floor():
    assert athena_cost_usd(1024**4) == pytest.approx(5.0)  # 1 TB -> $5
    assert athena_cost_usd(2 * 1024**4) == pytest.approx(10.0)


# --- run_query, against the hand-built fake ----------------------------------------------


def test_run_query_returns_rows_bytes_and_cost():
    client = _FakeAthenaClient(
        columns=["title", "source_id"],
        rows=[["A", "rss_tech"], ["B", "edgar"]],
        bytes_scanned=50 * 1024 * 1024,
        engine_ms=340,
    )
    result = run_query("SELECT title, source_id FROM silver.articles", client=client)

    assert result.rows == [
        {"title": "A", "source_id": "rss_tech"},
        {"title": "B", "source_id": "edgar"},
    ]
    assert result.bytes_scanned == 50 * 1024 * 1024
    assert result.engine_execution_ms == 340
    assert result.cost_usd == athena_cost_usd(50 * 1024 * 1024)
    assert client.started_with["WorkGroup"] == "signal"
    assert client.started_with["QueryExecutionContext"] == {"Database": "silver"}


def test_run_query_polls_until_a_terminal_state():
    client = _FakeAthenaClient(polls_before_terminal=3)
    run_query("SELECT 1", client=client, poll_interval_seconds=0)
    assert client.poll_count == 4  # 3 RUNNING + 1 terminal


def test_run_query_raises_on_failed_state_with_the_reason():
    client = _FakeAthenaClient(final_state="FAILED", reason="SYNTAX_ERROR: line 1:8")
    with pytest.raises(AthenaQueryFailed, match="SYNTAX_ERROR"):
        run_query("bad sql", client=client)


def test_run_query_raises_on_cancelled_state():
    client = _FakeAthenaClient(final_state="CANCELLED")
    with pytest.raises(AthenaQueryFailed, match="CANCELLED"):
        run_query("SELECT 1", client=client)


def test_run_query_times_out_rather_than_polling_forever():
    """A finite `polls_before_terminal` raced real wall-clock time: on a fast enough
    interpreter, 10,000 tight-loop polls at `poll_interval_seconds=0` could complete inside
    the 50 ms budget, so the fake would reach SUCCEEDED before `_await_completion` ever
    checked the deadline — a timeout test that could pass by never timing out, and whose
    result depended on how fast the machine running it happened to be.

    `float("inf")` removes the race rather than widening the budget to paper over it: the
    fake can now never reach a terminal state, so `TimeoutError` is the only way this loop
    can end, on any machine.
    """
    client = _FakeAthenaClient(polls_before_terminal=float("inf"))
    with pytest.raises(TimeoutError):
        run_query("SELECT 1", client=client, poll_interval_seconds=0, timeout_seconds=0.05)


def test_run_query_zero_rows_is_an_empty_list_not_a_crash():
    client = _FakeAthenaClient(columns=["title"], rows=[])
    result = run_query("SELECT title FROM silver.articles WHERE 1=0", client=client)
    assert result.rows == []


# --- run_query, against moto: proves the real boto3 call shapes ------------------------


def test_run_query_against_moto_exercises_the_real_boto3_surface():
    """moto's Athena backend has no query engine — `DataScannedInBytes` is always 0 —
    so this only proves `start_query_execution` / `get_query_execution` / the
    `get_query_results` paginator are called with shapes real boto3 accepts."""
    with mock_aws():
        client = boto3.client("athena", region_name="us-east-1")
        result = run_query(
            "SELECT 1",
            database="silver",
            workgroup="primary",
            client=client,
            poll_interval_seconds=0,
        )
        assert result.bytes_scanned == 0
        assert result.cost_usd == athena_cost_usd(0)  # still floored at the 10 MB minimum
        assert result.rows == []
