"""Keeping a deployed Iceberg table's columns in step with the DDL in the code.

`CREATE TABLE IF NOT EXISTS` creates a table once and then never looks at it again. That is
fine until a job's DDL grows a column, at which point the code and the deployed table drift
apart silently — the job keeps running, the new column is simply never written, and the
first thing to notice is whatever tries to *read* it.

Which is exactly how this module came to exist. 3.B.4 added `first_seen` and `last_seen` to
`silver.story_clusters` so a cluster could be timestamped by its most recent coverage rather
than its first report. The DDL changed, the tests passed against tables created fresh from
that DDL, and the deployed table — created days earlier — kept its original 17 columns. The
failure surfaced in 3.D, from the brief, as `COLUMN_NOT_FOUND: line 1:122: Column
'c.first_seen' cannot be resolved`, which is a long way from the change that caused it.

**Additive only, and deliberately so.** Adding a column is safe: existing rows read it as
null, existing writers ignore it. Dropping, renaming or retyping one is not — each can lose
data or break a reader mid-flight — so this reconciles the safe direction automatically and
leaves the rest to be a decision somebody makes on purpose.

Added columns are always **nullable**, whatever the DDL says. Iceberg will not add a required
column to a table that already has rows, because there is no value it could give them, and a
`NOT NULL` in a DDL is a statement about what a *writer* must supply rather than about what
history contains.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """`[(name, type), ...]` from one of the module-level DDL strings."""
    columns = []
    for line in ddl.strip().splitlines():
        stripped = line.strip().removesuffix(",")
        if not stripped:
            continue
        name, _, spec = stripped.partition(" ")
        columns.append((name, spec.replace(" NOT NULL", "").strip()))
    return columns


def ensure_columns(spark: SparkSession, table: str, ddl: str) -> list[str]:
    """Add any column the DDL declares and the table lacks. Returns what was added.

    Returned rather than logged because a schema that just changed under a running pipeline
    is something a person should see. `cluster_window` and `resolve_window` put it in their
    result objects, so it reaches the DAG's task output instead of a log nobody opens.
    """
    existing = {field.name.lower() for field in spark.table(table).schema.fields}
    added = []
    for name, spec in ddl_columns(ddl):
        if name.lower() in existing:
            continue
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {spec}")
        added.append(name)
    return added
