"""`gold.brief_items` — the row that makes the feedback loop a loop. 4A.H, 4A.I."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from signal_core.brief.items import write_brief_items

NOW = datetime(2026, 8, 22, 6, 0, tzinfo=UTC)


class _Paginator:
    """Athena's first row is the column header, not data — including on a zero-row result."""

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
    """Records every SQL and answers SELECTs from a fixed row list.

    Enough to assert on the statements themselves, which is the point: the DELETE's WHERE
    clause is the whole reason a mark survives a re-run, and it is invisible in the return
    value."""

    def __init__(self, existing: list[dict[str, str | None]] | None = None) -> None:
        self.existing = existing or []
        self.queries: list[str] = []
        self._current = ""

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self._current = kwargs["QueryString"]
        self.queries.append(self._current)
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
        wanted = self.existing if "SELECT cluster_id" in self._current else []
        return _Paginator(["cluster_id"], [[row["cluster_id"]] for row in wanted])

    def sql_containing(self, needle: str) -> str:
        return next(q for q in self.queries if needle in q)


def _ranked(**overrides) -> dict[str, Any]:
    base = {
        "cluster_id": "c1",
        "title": "Northwind acquires Lumen Robotics",
        "score": 0.62,
        "score_components": {"breadth": 1.0, "recency": 0.5},
        "rank": 1,
        "included": True,
    }
    return {**base, **overrides}


def test_only_what_was_shown_is_recorded():
    """`rank` positions every cluster and cuts at `limit`. Writing the tail would mean a
    table where almost every row describes a story nobody saw."""
    client = _FakeAthena()
    written = write_brief_items(
        [_ranked(), _ranked(cluster_id="c2", rank=2, included=False)],
        date="2026-08-22",
        now=NOW,
        client=client,
    )
    assert written == 1
    assert "'c1'" in client.sql_containing("INSERT INTO")
    assert "'c2'" not in client.sql_containing("INSERT INTO")


def test_score_components_are_stored_as_a_map():
    """Storing the weighted total per component would bake today's WEIGHTS into a row that
    outlives them."""
    client = _FakeAthena()
    write_brief_items([_ranked()], date="2026-08-22", now=NOW, client=client)

    insert = client.sql_containing("INSERT INTO")
    assert "MAP(ARRAY['breadth', 'recency'], ARRAY[1.0, 0.5])" in insert


def test_a_rerun_replaces_unmarked_rows_but_keeps_marked_ones():
    """The property the DELETE's WHERE clause exists for: `make brief` twice in a morning
    must not double the rows, and must not discard a mark already left."""
    client = _FakeAthena()
    write_brief_items([_ranked()], date="2026-08-22", now=NOW, client=client)

    delete = client.sql_containing("DELETE FROM")
    assert "brief_date = date '2026-08-22'" in delete
    assert "user_feedback IS NULL" in delete, "a marked row must survive a re-run"


def test_a_cluster_already_marked_today_is_not_re_inserted():
    """Otherwise a re-run would leave two rows for one story — the surviving marked one and
    a fresh unmarked duplicate — and the feedback read would see both."""
    client = _FakeAthena(existing=[{"cluster_id": "c1"}])
    written = write_brief_items(
        [_ranked(), _ranked(cluster_id="c2", rank=2)],
        date="2026-08-22",
        now=NOW,
        client=client,
    )
    assert written == 1
    insert = client.sql_containing("INSERT INTO")
    assert "'c2'" in insert
    assert "'c1'" not in insert


def test_an_apostrophe_in_a_headline_does_not_break_the_insert():
    """Not an edge case — "Finance's Cushiest Jobs" is in the committed fixtures."""
    client = _FakeAthena()
    write_brief_items(
        [_ranked(title="AI Is Upending One of Finance's Cushiest Jobs")],
        date="2026-08-22",
        now=NOW,
        client=client,
    )
    assert "'AI Is Upending One of Finance''s Cushiest Jobs'" in client.sql_containing("INSERT")


def test_nothing_shown_writes_nothing():
    """An empty brief should not leave a DELETE behind that a later read would notice."""
    client = _FakeAthena()
    assert write_brief_items([], date="2026-08-22", now=NOW, client=client) == 0
    assert client.queries == []


def test_the_table_is_created_on_first_use():
    """A fresh environment's first brief should not need a separate setup step."""
    client = _FakeAthena()
    write_brief_items([_ranked()], date="2026-08-22", now=NOW, client=client)
    assert "CREATE TABLE IF NOT EXISTS gold.brief_items" in client.sql_containing("CREATE TABLE")
    # Quoted key: `TBLPROPERTIES ('table_type' = 'ICEBERG')`. This assertion used to check
    # the unquoted `WITH (table_type = ...)` form, which Athena rejects — it was pinning the
    # bug rather than the behaviour, because the fake client never parses what it is given.
    assert "'table_type' = 'ICEBERG'" in client.sql_containing("CREATE TABLE")


@pytest.mark.parametrize("mark", ["up", "down"])
def test_feedback_reads_back_the_marks_it_wrote(mark):
    """The loop closing: `read_feedback` has to understand what `signal brief feedback`
    stores, and both sides are pinned here rather than agreeing by coincidence."""
    from signal_core.brief.read import _FEEDBACK_SCORES

    assert mark in _FEEDBACK_SCORES
    assert _FEEDBACK_SCORES[mark] in (1.0, -1.0)


# --- the dialect ---------------------------------------------------------------------------
#
# These exist because 4A's DDL was wrong in three separate ways and shipped anyway. The fake
# Athena client above records SQL without parsing it — which is what makes it useful for
# asserting on statements, and exactly why it cannot catch a syntax error. A static check
# against Athena's documented Iceberg type set is the cheap half of the gap.


# What Athena accepts in a `CREATE TABLE` for an Iceberg table. Notably absent: `varchar`,
# `integer`, `bigint` — all valid Trino *expression* types, none valid here.
ATHENA_ICEBERG_TYPES = frozenset(
    {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "decimal",
        "string",
        "uuid",
        "binary",
        "date",
        "timestamp",
        "array",
        "map",
        "struct",
    }
)


def _declared_types(ddl: str) -> list[str]:
    return [
        line.strip().rstrip(",").split(" ", 1)[1].strip().split("<")[0].split("(")[0].lower()
        for line in ddl.strip().splitlines()
        if line.strip()
    ]


@pytest.mark.parametrize("ddl_name", ["BRIEF_ITEMS_DDL"])
def test_every_column_type_is_one_athena_accepts(ddl_name: str):
    """`rank integer` and `cluster_id varchar` both parse fine in a SELECT and both are
    rejected by `CREATE TABLE`, with `type expected at the position 0 of 'integer'` — a
    message that names the type but not the column or the table."""
    from signal_core.brief import items

    for declared in _declared_types(getattr(items, ddl_name)):
        assert declared in ATHENA_ICEBERG_TYPES, f"{declared!r} is not an Athena Iceberg type"


def test_the_create_uses_location_and_tblproperties_not_with():
    """`WITH (table_type = 'ICEBERG')` is Trino's CTAS property syntax and Athena rejects it
    with `no viable alternative at input`. `LOCATION` is required rather than defaulted from
    the Glue database, which is the part most likely to be dropped as redundant."""

    athena = _FakeAthena()
    from signal_core.brief.items import ensure_table

    ensure_table(client=athena)
    create = athena.sql_containing("CREATE TABLE")

    assert "TBLPROPERTIES" in create
    assert "'table_type' = 'ICEBERG'" in create
    assert "WITH (" not in create
    assert "LOCATION '" in create


def test_the_table_lands_where_spark_would_have_put_it():
    """`<warehouse>/<namespace>.db/<table>`, matching the layout `bronze.db/`, `silver.db/`
    and `ops.db/` already use. Getting this wrong does not fail — it quietly scatters a
    second copy of the lake elsewhere in the bucket."""
    from signal_core.ops.athena import iceberg_table_location

    assert (
        iceberg_table_location("gold.brief_items", "s3://bucket/warehouse")
        == "s3://bucket/warehouse/gold.db/brief_items"
    )
    assert (
        iceberg_table_location("gold.brief_items", "s3://bucket/warehouse/")
        == "s3://bucket/warehouse/gold.db/brief_items"
    )
