"""`signal feedback` — the recording half of SPEC §12's 4A acceptance. 4A.I."""

from __future__ import annotations

from typing import Any

import pytest

from signal_core.cli_feedback import run_feedback, run_list


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
    """Answers SELECTs from a fixed item list and remembers what was UPDATEd.

    `stored` is mutated by the UPDATE so the read-back sees the new value — which is the
    behaviour the CLI depends on and the reason it reads back at all."""

    def __init__(self, items: list[dict[str, str | None]] | None = None) -> None:
        self.items = items if items is not None else []
        self.queries: list[str] = []
        self._current = ""

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self._current = kwargs["QueryString"]
        self.queries.append(self._current)
        if self._current.lstrip().upper().startswith("UPDATE"):
            value = self._current.split("user_feedback = ", 1)[1].split(" ", 1)[0]
            for item in self.items:
                item["user_feedback"] = None if value == "NULL" else value.strip("'")
        return {"QueryExecutionId": "x"}

    def get_query_execution(self, QueryExecutionId: str) -> dict[str, Any]:
        del QueryExecutionId
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": 1024, "EngineExecutionTimeInMillis": 5},
            }
        }

    def get_paginator(self, operation_name: str) -> _Paginator:
        assert operation_name == "get_query_results"
        if "SELECT user_feedback" in self._current:
            return _Paginator(["user_feedback"], [[i["user_feedback"]] for i in self.items])
        if "SELECT cluster_id, rank, title" in self._current:
            # `_resolve`'s prefix lookup. Filtered here rather than answered wholesale so a
            # test can assert that an ambiguous prefix is refused.
            prefix = self._current.split("LIKE '", 1)[1].split("%'", 1)[0]
            columns = ["cluster_id", "rank", "title"]
            rows = [i for i in self.items if (i.get("cluster_id") or "").startswith(prefix)]
            return _Paginator(columns, [[i.get(c) for c in columns] for i in rows])
        if "SELECT rank, title" in self._current:
            columns = ["rank", "title", "user_feedback"]
            return _Paginator(columns, [[i[c] for c in columns] for i in self.items])
        if "SELECT rank, cluster_id" in self._current:
            columns = ["rank", "cluster_id", "title", "score", "user_feedback"]
            return _Paginator(columns, [[i.get(c) for c in columns] for i in self.items])
        return _Paginator([], [])

    def sql_containing(self, needle: str) -> str:
        return next(q for q in self.queries if needle in q)


def _item(**overrides) -> dict[str, str | None]:
    base = {
        "rank": "1",
        "cluster_id": "c1",
        "title": "Northwind acquires Lumen Robotics",
        "score": "0.62",
        "user_feedback": None,
    }
    return {**base, **overrides}


@pytest.mark.parametrize("mark", ["up", "down"])
def test_a_mark_is_written_and_read_back(mark, capsys):
    client = _FakeAthena([_item()])
    assert run_feedback("2026-08-22", "c1", mark, client=client) == 0

    update = client.sql_containing("UPDATE")
    assert f"user_feedback = '{mark}'" in update
    assert "brief_date = date '2026-08-22'" in update
    # Read back rather than trusted: Athena's UPDATE reports no affected-row count.
    assert f"->  {mark}" in capsys.readouterr().out


def test_clear_removes_a_mark():
    client = _FakeAthena([_item(user_feedback="up")])
    assert run_feedback("2026-08-22", "c1", "clear", client=client) == 0
    assert "user_feedback = NULL" in client.sql_containing("UPDATE")


def test_an_unknown_cluster_fails_loudly_rather_than_succeeding_silently(capsys):
    """The reason the CLI reads before it writes. Athena's UPDATE reports no affected-row
    count, so a typo'd id would otherwise exit 0 and the reader would believe a mark was
    recorded that never was."""
    client = _FakeAthena([])
    assert run_feedback("2026-08-22", "nope", "up", client=client) == 1
    assert not any(q.lstrip().upper().startswith("UPDATE") for q in client.queries)
    out = capsys.readouterr().out
    # Phrased as "no item starting with" since 5.C made short ids resolvable; what the test
    # is actually about is that nothing was written and the id is named back to the reader.
    assert "no item" in out and "nope" in out


def test_an_unknown_mark_is_rejected_before_any_query(capsys):
    client = _FakeAthena([_item()])
    assert run_feedback("2026-08-22", "c1", "sideways", client=client) == 2
    assert client.queries == []
    assert "unknown mark" in capsys.readouterr().out


def test_an_apostrophe_in_a_cluster_id_is_escaped():
    """Cluster ids are content hashes today, but the escaping is not conditional on that."""
    client = _FakeAthena([_item()])
    run_feedback("2026-08-22", "o'brien", "up", client=client)
    assert "'o''brien'" in client.sql_containing("SELECT rank, title")


def test_list_shows_what_the_brief_contained(capsys):
    """The verb that needs a cluster id has to be able to show them — they are content
    hashes, not something anyone types from memory."""
    client = _FakeAthena([_item(), _item(rank="2", cluster_id="c2", title="Second", score="0.5")])
    assert run_list("2026-08-22", client=client) == 0

    out = capsys.readouterr().out
    assert "c1" in out and "c2" in out
    assert "Northwind" in out
    assert "bytes scanned" in out, "SPEC §10.3: a query that costs money says so"


def test_list_on_a_date_with_no_brief_is_an_error_not_an_empty_success(capsys):
    client = _FakeAthena([])
    assert run_list("2026-01-01", client=client) == 1
    assert "no brief items recorded" in capsys.readouterr().out


# --- short ids (5.C) ------------------------------------------------------------------------


def test_a_short_id_read_off_the_page_resolves_to_the_full_one():
    """ADR-0015 measured one mark across 60 items and named the cause: the page printed no
    cluster id, so marking meant copying a 64-character hash out of a table the reader could
    not see. The brief now prints eight characters and this resolves them."""
    full = "c98e41f4" + "0" * 56
    client = _FakeAthena([_item(cluster_id=full)])
    assert run_feedback("2026-08-22", "c98e41f4", "up", client=client) == 0
    assert f"cluster_id = '{full}'" in client.sql_containing("UPDATE")


def test_an_ambiguous_prefix_is_refused_rather_than_guessed(capsys):
    """A wrong mark feeds `ranker`'s feedback component, the one term that can subtract."""
    client = _FakeAthena(
        [
            _item(cluster_id="abc11111" + "0" * 56, rank="1", title="First"),
            _item(cluster_id="abc22222" + "0" * 56, rank="2", title="Second"),
        ]
    )
    assert run_feedback("2026-08-22", "abc", "up", client=client) == 1
    assert not any(q.lstrip().upper().startswith("UPDATE") for q in client.queries)
    out = capsys.readouterr().out
    assert "ambiguous" in out and "abc11111" in out and "abc22222" in out


def test_a_full_id_still_works_without_a_resolve_query():
    full = "d" * 64
    client = _FakeAthena([_item(cluster_id=full)])
    assert run_feedback("2026-08-22", full, "up", client=client) == 0
    assert not any("LIKE" in q for q in client.queries)
