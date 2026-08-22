"""`gold.cluster_enrichment` and `gold.enrichment_rejects`. SPEC §7.3, §9.

Written through Athena rather than Spark, for the reason `brief/items.py` already argues at
length: the pre-brief path stays JVM-free, Athena engine v3 (Trino) writes Iceberg directly,
and starting a JVM to insert forty rows is machinery this stage does not need.

## What `cache_hit` means on a row

SPEC §9 lists `cache_hit` as a column on `cluster_enrichment`, and the obvious reading — "was
this served from cache" — cannot be a property of a stored row, because a row exists exactly
when an inference happened. The column is kept and given the meaning that is actually useful:

**`cache_hit` is true when this row's content was taken from an existing identical
`input_hash` belonging to a *different* cluster**, rather than from a fresh generation. That
is a real event — two clusters whose head text is identical, which syndication produces —
and it is the one kind of hit a stored row can honestly record.

The **published** hit rate (§11, and the brief's health footer) is a different, run-level
number that `run.py` computes: how many of the clusters this run had to enrich were satisfied
without calling the model at all, counting both same-cluster re-runs and cross-cluster
matches. A row cannot record that, so it is reported by the runner rather than derived here.

## Column drift

`ensure_tables` reconciles against `information_schema` rather than trusting
`CREATE TABLE IF NOT EXISTS`. That statement is a no-op against a live table, so a column
added to a DDL never reaches a deployed one — 3.D found this on `silver.articles` and 4A
found it *again* on `ops.maintenance_runs`, both times with every test passing, because tests
create their tables fresh from the current DDL. Two occurrences in two phases is a pattern,
so the third table family gets the guard on the way in rather than after the incident.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from signal_core.config import Settings
from signal_core.enrich.schema import Enrichment
from signal_core.ops.athena import (
    QueryResult,
    create_iceberg_table,
    run_query,
    sql_string,
)
from signal_core.timeutil import utc_now

settings = Settings()

# What a read that never ran costs. Returned rather than None so callers can sum `.queries`
# without a special case, matching `brief/select.py::EMPTY_QUERY`.
EMPTY_RESULT = QueryResult(rows=[], bytes_scanned=0, engine_execution_ms=0, cost_usd=0.0)

CLUSTER_ENRICHMENT_TABLE = "gold.cluster_enrichment"
ENRICHMENT_REJECTS_TABLE = "gold.enrichment_rejects"

# SPEC §9's schema, in its order. `extracted_json` is a varchar rather than a struct on
# purpose: the extraction shape is versioned by `prompt_version` and will change, and a
# struct column would turn every schema revision into an Iceberg migration. The typed
# guarantee lives in `schema.Enrichment`, which is what validates before anything is written.
CLUSTER_ENRICHMENT_DDL = """
    cluster_id string,
    model_name string,
    model_digest string,
    prompt_version string,
    summary string,
    topic string,
    extracted_json string,
    generated_at timestamp,
    input_hash string,
    cache_hit boolean
"""

# SPEC §9 again. `attempts` is not in the spec's list and is added deliberately: §7.3 says
# failures are "never retried indefinitely", and a bound needs somewhere to count against.
ENRICHMENT_REJECTS_DDL = """
    cluster_id string,
    input_hash string,
    raw_output string,
    validation_error string,
    model_digest string,
    prompt_version string,
    rejected_at timestamp,
    attempts int
"""

# How many times one (cluster, input_hash) may be re-attempted across runs before the stage
# stops spending inference on it. SPEC §7.3: "never retried indefinitely". Three is enough to
# ride out a transient bad decode and small enough that a genuinely unparseable head costs
# three calls total rather than one per morning forever.
MAX_ATTEMPTS = 3

# Raw output is quarantined so a person can read what the model actually said, but a
# degenerate repeat loop can emit a very large string and there is no value in storing all of
# it — the first few thousand characters show the shape of the failure.
RAW_OUTPUT_MAX_CHARS = 4000


@dataclass(frozen=True)
class CachedEnrichment:
    """A row read back out of `gold.cluster_enrichment`."""

    cluster_id: str
    input_hash: str
    summary: str
    topic: str
    extraction: dict[str, Any]


def _columns(table: str, *, database: str, workgroup: str, client: Any | None) -> set[str]:
    namespace, name = table.rsplit(".", 1)
    result = run_query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = {sql_string(namespace)} AND table_name = {sql_string(name)}",
        database=database,
        workgroup=workgroup,
        client=client,
    )
    columns: set[str] = set()
    for row in result.rows:
        column = row.get("column_name")
        if column:
            columns.add(column)
    return columns


def _ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """(name, type) pairs from a DDL block, in declaration order.

    Mirrors `spark/tables.py::ddl_columns`. Same caveat as that one: a `--` comment inside the
    DDL string would be read as a column name, so comments go outside it.
    """
    pairs = []
    for line in ddl.strip().splitlines():
        field = line.strip().rstrip(",")
        if not field:
            continue
        name, _, type_name = field.partition(" ")
        pairs.append((name, type_name.strip()))
    return pairs


def _ensure_one_table(
    table: str, ddl: str, *, database: str, workgroup: str, client: Any | None
) -> list[str]:
    """Create the table if absent, add any column the DDL has gained. Returns what was added."""
    create_iceberg_table(
        table,
        ddl,
        warehouse=settings.iceberg_warehouse,
        database=database,
        workgroup=workgroup,
        client=client,
    )

    existing = _columns(table, database=database, workgroup=workgroup, client=client)
    if not existing:
        # `information_schema` can lag a just-created table. Nothing to reconcile against,
        # and guessing that every column is missing would ALTER a correct table.
        return []

    added = []
    for name, type_name in _ddl_columns(ddl):
        if name.lower() not in {c.lower() for c in existing}:
            run_query(
                f"ALTER TABLE {table} ADD COLUMNS ({name} {type_name})",
                database=database,
                workgroup=workgroup,
                client=client,
            )
            added.append(name)
    return added


def ensure_tables(
    *,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> dict[str, list[str]]:
    """Idempotent create-and-migrate for both gold tables. Returns columns added per table."""
    database = database or settings.athena_database
    workgroup = workgroup or settings.athena_workgroup
    return {
        table: _ensure_one_table(table, ddl, database=database, workgroup=workgroup, client=client)
        for table, ddl in (
            (CLUSTER_ENRICHMENT_TABLE, CLUSTER_ENRICHMENT_DDL),
            (ENRICHMENT_REJECTS_TABLE, ENRICHMENT_REJECTS_DDL),
        )
    }


def read_rows(
    input_hashes: list[str],
    *,
    model_digest: str,
    prompt_version: str,
    table: str = CLUSTER_ENRICHMENT_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[list[CachedEnrichment], QueryResult]:
    """Every stored enrichment for these input hashes, one entry per row.

    **Filtered on `model_digest` and `prompt_version` as well as the hash**, which is the
    same three-part key `hashing.enrichment_cache_key` builds. Reading on the hash alone
    would serve output the current configuration would not have produced, and would make the
    hit rate a statistic about the past rather than a metric (SPEC §7.3).

    Returns rows rather than a hash-keyed map because the runner needs to distinguish "this
    cluster already has a row" from "a *different* cluster has one with identical text" —
    the first needs no write, the second writes a row with `cache_hit` true. Collapsing to
    one entry per hash would lose exactly that distinction.

    Returns the `QueryResult` alongside, the same shape every `brief/read.py` reader has, so
    the bytes this costs land in the brief's footer instead of going unaccounted (SPEC §17).
    """
    if not input_hashes:
        return [], EMPTY_RESULT
    wanted = ", ".join(sql_string(h) for h in sorted(set(input_hashes)))
    result = run_query(
        f"SELECT cluster_id, input_hash, summary, topic, extracted_json FROM {table} "
        f"WHERE input_hash IN ({wanted}) "
        f"AND model_digest = {sql_string(model_digest)} "
        f"AND prompt_version = {sql_string(prompt_version)}",
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )
    rows = []
    for row in result.rows:
        key = row.get("input_hash")
        if not key:
            continue
        try:
            extraction = json.loads(row.get("extracted_json") or "{}")
        except ValueError:
            # A row whose JSON no longer parses is not a usable cache entry. Skipping it
            # means one re-inference, which is strictly better than serving the brief a
            # value this stage cannot read.
            continue
        rows.append(
            CachedEnrichment(
                cluster_id=row.get("cluster_id") or "",
                input_hash=key,
                summary=row.get("summary") or "",
                topic=row.get("topic") or "",
                extraction=extraction,
            )
        )
    return rows, result


def read_cached(
    input_hashes: list[str],
    *,
    model_digest: str,
    prompt_version: str,
    table: str = CLUSTER_ENRICHMENT_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[dict[str, CachedEnrichment], QueryResult]:
    """`read_rows`, collapsed to one entry per `input_hash`.

    What the brief wants: the head text hashes to one key, and any row carrying that key is
    the right content regardless of which cluster caused it to be generated.
    """
    rows, query = read_rows(
        input_hashes,
        model_digest=model_digest,
        prompt_version=prompt_version,
        table=table,
        database=database,
        workgroup=workgroup,
        client=client,
    )
    return {row.input_hash: row for row in rows}, query


def read_attempts(
    input_hashes: list[str],
    *,
    table: str = ENRICHMENT_REJECTS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> dict[str, int]:
    """How many times each input hash has already failed validation. SPEC §7.3's retry bound."""
    if not input_hashes:
        return {}
    wanted = ", ".join(sql_string(h) for h in sorted(set(input_hashes)))
    result = run_query(
        f"SELECT input_hash, max(attempts) AS attempts FROM {table} "
        f"WHERE input_hash IN ({wanted}) GROUP BY input_hash",
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )
    counts = {}
    for row in result.rows:
        key = row.get("input_hash")
        if key:
            counts[key] = int(row.get("attempts") or 0)
    return counts


def write_enrichments(
    rows: list[tuple[str, str, Enrichment, bool]],
    *,
    model_name: str,
    model_digest: str,
    prompt_version: str,
    now: datetime | None = None,
    table: str = CLUSTER_ENRICHMENT_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> int:
    """Insert `(cluster_id, input_hash, enrichment, cache_hit)` rows. Returns rows written."""
    if not rows:
        return 0
    now = now or utc_now()
    values = ",\n            ".join(
        "("
        f"{sql_string(cluster_id)}, "
        f"{sql_string(model_name)}, "
        f"{sql_string(model_digest)}, "
        f"{sql_string(prompt_version)}, "
        f"{sql_string(enrichment.summary)}, "
        f"{sql_string(enrichment.topic.value)}, "
        f"{sql_string(enrichment.extraction.model_dump_json())}, "
        f"timestamp '{now:%Y-%m-%d %H:%M:%S}', "
        f"{sql_string(input_hash)}, "
        f"{'true' if cache_hit else 'false'}"
        ")"
        for cluster_id, input_hash, enrichment, cache_hit in rows
    )
    run_query(
        f"INSERT INTO {table} VALUES\n            {values}",
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )
    return len(rows)


def write_rejects(
    rows: list[tuple[str, str, str, str, int]],
    *,
    model_digest: str,
    prompt_version: str,
    now: datetime | None = None,
    table: str = ENRICHMENT_REJECTS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> int:
    """Quarantine `(cluster_id, input_hash, raw_output, validation_error, attempts)` rows.

    SPEC §7.3: never silently dropped. A reject is a record that the stage produced something
    and refused it, which is a different and more useful fact than an absent row.
    """
    if not rows:
        return 0
    now = now or utc_now()
    values = ",\n            ".join(
        "("
        f"{sql_string(cluster_id)}, "
        f"{sql_string(input_hash)}, "
        f"{sql_string((raw_output or '')[:RAW_OUTPUT_MAX_CHARS])}, "
        f"{sql_string(error)}, "
        f"{sql_string(model_digest)}, "
        f"{sql_string(prompt_version)}, "
        f"timestamp '{now:%Y-%m-%d %H:%M:%S}', "
        f"{int(attempts)}"
        ")"
        for cluster_id, input_hash, raw_output, error, attempts in rows
    )
    run_query(
        f"INSERT INTO {table} VALUES\n            {values}",
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )
    return len(rows)
