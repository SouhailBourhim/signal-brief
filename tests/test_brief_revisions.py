"""Recent macro revisions in the brief. SPEC §8; 4B.J.

The README's lead differentiator, reduced to the one line a reader actually sees:
"payrolls revised down 46k across the prior two months". These tests are about the query
being asked correctly — the axis it filters on, and what it refuses to call a revision.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from signal_core.brief.read import REVISION_LOOKBACK_DAYS, read_macro_revisions

NOW = datetime(2026, 8, 22, 6, 0, tzinfo=UTC)

COLUMNS = ["series_id", "period", "value", "revision_delta", "vintage_date"]


class _Paginator:
    def __init__(self, columns: list[str], rows: list[list[str | None]]) -> None:
        self._columns, self._rows = columns, rows

    def paginate(self, **_: Any):
        yield {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": c} for c in self._columns]},
                    *(
                        {"Data": [({} if v is None else {"VarCharValue": v}) for v in row]}
                        for row in self._rows
                    ),
                ]
            }
        }


class _FakeAthena:
    def __init__(self, rows: list[list[str | None]] | None = None) -> None:
        self.rows = rows or []
        self.queries: list[str] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs["QueryString"])
        return {"QueryExecutionId": "x"}

    def get_query_execution(self, QueryExecutionId: str) -> dict[str, Any]:
        del QueryExecutionId
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": 0, "EngineExecutionTimeInMillis": 1},
            }
        }

    def get_paginator(self, operation_name: str) -> _Paginator:
        assert operation_name == "get_query_results"
        return _Paginator(COLUMNS, self.rows)

    @property
    def sql(self) -> str:
        return self.queries[0]


def test_a_revision_carries_both_the_new_and_the_previous_value():
    """§8's line names both numbers. `previous_value` is derived from the delta rather than
    fetched, because the two must agree — reading them independently invites a brief that
    states an arithmetic impossibility."""
    athena = _FakeAthena([["PAYEMS", "2026-06-01", "159796", "-46", "2026-08-07"]])
    revisions, _ = read_macro_revisions(NOW, client=athena)

    assert len(revisions) == 1
    revision = revisions[0]
    assert revision.series_id == "PAYEMS"
    assert revision.period == date(2026, 6, 1)
    assert revision.value == 159796.0
    assert revision.previous_value == 159842.0
    assert revision.revision_delta == -46.0
    assert revision.vintage_date == date(2026, 8, 7)


def test_the_filter_is_on_vintage_date_not_period():
    """The two axes come apart precisely when it matters. A benchmark revision issued this
    week can restate a figure from eighteen months ago, and filtering on `period` would drop
    exactly that case while keeping every unrevised recent month."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)

    assert "vintage_date >=" in athena.sql
    assert "period >=" not in athena.sql


def test_only_the_current_vintage_of_a_period_is_reported():
    """A period revised three times should contribute one line, not three competing claims.
    The full audit trail stays in the table for anyone asking a bitemporal question."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)
    assert "is_latest" in athena.sql


def test_a_zero_delta_is_excluded_at_the_query():
    """ "Revised by zero" is not a revision, and a brief line saying a number moved by nothing
    is worse than no line."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)
    assert "revision_delta <> 0" in athena.sql


def test_rounding_sized_revisions_are_excluded_proportionally():
    """Stated as a fraction of the value, not an absolute: these series have wildly different
    units — payrolls in thousands, the fed funds rate in percent — and one absolute threshold
    cannot serve both without silencing one series or flooding the other."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)
    assert "abs(revision_delta) >= abs(value)" in athena.sql


def test_the_lookback_is_the_stated_window():
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)
    assert REVISION_LOOKBACK_DAYS == 45
    assert "date '2026-07-08'" in athena.sql


def test_the_biggest_proportional_move_leads():
    """Ordered by relative size for the same reason the threshold is relative: an absolute
    ordering would put payrolls above every rate series on every single day."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, client=athena)
    assert "ORDER BY abs(revision_delta / nullif(value, 0)) DESC" in athena.sql


def test_a_row_missing_either_time_axis_is_skipped_rather_than_rendered():
    """A revision with no period is unplaceable, and a brief line reading "revised for None"
    is worse than one fewer line."""
    athena = _FakeAthena(
        [
            ["PAYEMS", None, "159796", "-46", "2026-08-07"],
            ["UNRATE", "2026-06-01", "4.1", "0.1", None],
            ["CPIAUCSL", "2026-06-01", "320.1", "0.4", "2026-08-07"],
        ]
    )
    revisions, _ = read_macro_revisions(NOW, client=athena)

    assert [r.series_id for r in revisions] == ["CPIAUCSL"]


def test_no_revisions_is_an_empty_list_not_a_failure():
    """An ordinary month. Most days have no new vintage at all."""
    revisions, query = read_macro_revisions(NOW, client=_FakeAthena())
    assert revisions == []
    assert query.bytes_scanned == 0


def test_the_query_is_capped():
    """A brief is useful because of what it omits (SPEC §7.4). A benchmark revision restates
    dozens of periods at once, and printing all of them would bury the stories."""
    athena = _FakeAthena()
    read_macro_revisions(NOW, limit=5, client=athena)
    assert "LIMIT 5" in athena.sql
