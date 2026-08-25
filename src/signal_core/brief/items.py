"""`gold.brief_items` — what the reader was actually shown, and what they thought of it.

SPEC §9's schema: `brief_date, rank, cluster_id, score, score_components, included,
user_feedback`. Three jobs, and the third is the one 4A's acceptance test turns on:

1. It records the ranking decision, `score_components` included, so "why was this #1 on
   Tuesday" is answerable later rather than being re-derivable only if nothing changed.
2. It is what `signal brief feedback` updates.
3. It is what the next run's `feedback` component reads back — the loop SPEC §12's 4A
   acceptance means by "the feedback loop records your marks".

## Written through Athena, not Spark

`brief/build.py` and `brief/read.py` stay JVM-free on purpose — their own docstrings make
the argument that starting Spark to render ten stories is a lot of machinery for a SELECT.
Athena engine v3 (Trino) creates and writes Iceberg tables directly, so this uses the same
`ops/athena.py::run_query` primitive every read already goes through and adds no JVM boot
to the 16:00 path.

## Why DELETE-then-INSERT rather than MERGE

Re-running `make brief` for the same date must not double the rows, and it must not silently
discard a mark the reader already left. So the write deletes only rows for `brief_date` that
carry **no feedback**, then inserts the fresh ranking. A marked row survives a re-run; an
unmarked one is replaced by the newer ranking of the same day. MERGE would express the first
half and not the second, because a re-run legitimately changes `rank` and `score` for rows
that should still be there.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from signal_core.config import Settings
from signal_core.ops.athena import create_iceberg_table, run_query, sql_string
from signal_core.timeutil import brief_date, utc_now

settings = Settings()

BRIEF_ITEMS_TABLE = "gold.brief_items"

# `score_components` is a map so the explanation survives a weight change: storing the
# weighted total per component would bake today's `WEIGHTS` into a row that outlives them.
#
# **`map<varchar, double>`, with angle brackets, not `map(varchar, double)`.** Athena's
# `CREATE TABLE` takes Hive-style type syntax; the parenthesised form is Trino's *expression*
# type syntax and is only valid in a `SELECT`/`INSERT`. Athena rejects the wrong one with
# `no viable alternative at input`, which names neither the column nor the reason.
#
# This shipped broken in 4A and was found on 2026-08-23 by running the brief against the real
# account. Nothing caught it: the tests inject a fake Athena client that records SQL without
# parsing it, and the `brief` DAG that would have executed it had been paused since it was
# written. `gold.brief_items` therefore never existed, and 4A's feedback loop — the thing its
# acceptance turns on — had never once worked.
BRIEF_ITEMS_DDL = """
    brief_date date,
    rank int,
    cluster_id string,
    title string,
    score double,
    score_components map<string, double>,
    included boolean,
    user_feedback string,
    created_at timestamp
"""


def _components_map(components: dict[str, float]) -> str:
    if not components:
        return "MAP(ARRAY[], ARRAY[])"
    keys = ", ".join(sql_string(k) for k in sorted(components))
    values = ", ".join(f"{float(components[k])}" for k in sorted(components))
    return f"MAP(ARRAY[{keys}], ARRAY[{values}])"


def ensure_table(
    *,
    table: str = BRIEF_ITEMS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> None:
    """Idempotent, and run on every build — the same posture `spark/jobs`' `ensure_table`
    functions take, for the same reason: the first brief in a fresh environment should not
    need a separate setup step."""
    create_iceberg_table(
        table,
        BRIEF_ITEMS_DDL,
        warehouse=settings.iceberg_warehouse,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )


def write_brief_items(
    ranked: list[dict[str, Any]],
    *,
    date: str | None = None,
    now: datetime | None = None,
    table: str = BRIEF_ITEMS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> int:
    """Record one row per ranked cluster. Returns rows written.

    Only the clusters that were *shown* are recorded. `rank` assigns a position to every
    cluster and cuts at `limit`, and writing the tail would mean a table where 99% of rows
    describe stories nobody saw — the feedback component would then read marks against
    positions that never appeared on a page.
    """
    now = now or utc_now()
    day = date or brief_date(now)
    shown = [c for c in ranked if c.get("included")]
    if not shown:
        return 0

    database = database or settings.athena_database
    workgroup = workgroup or settings.athena_workgroup
    ensure_table(table=table, database=database, workgroup=workgroup, client=client)

    # See the module docstring: unmarked rows for this date are replaced, marked ones stay.
    run_query(
        f"DELETE FROM {table} WHERE brief_date = date '{day}' AND user_feedback IS NULL",
        database=database,
        workgroup=workgroup,
        client=client,
    )

    already_marked = run_query(
        f"SELECT cluster_id FROM {table} WHERE brief_date = date '{day}'",
        database=database,
        workgroup=workgroup,
        client=client,
    )
    marked = {row.get("cluster_id") for row in already_marked.rows}

    rows = [c for c in shown if c["cluster_id"] not in marked]
    if not rows:
        return 0

    values = ",\n            ".join(
        "("
        f"date '{day}', "
        f"{int(cluster['rank'])}, "
        f"{sql_string(cluster['cluster_id'])}, "
        f"{sql_string(cluster.get('title'))}, "
        f"{float(cluster.get('score', 0.0))}, "
        f"{_components_map(cluster.get('score_components') or {})}, "
        f"{'true' if cluster.get('included') else 'false'}, "
        "NULL, "
        f"timestamp '{now:%Y-%m-%d %H:%M:%S}'"
        ")"
        for cluster in rows
    )
    run_query(
        f"INSERT INTO {table} VALUES\n            {values}",
        database=database,
        workgroup=workgroup,
        client=client,
    )
    return len(rows)
