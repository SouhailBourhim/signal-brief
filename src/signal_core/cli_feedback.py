"""`signal feedback` — the reader's morning marks. SPEC §7.4, §12's 4A acceptance.

The acceptance test for 4A is "you read it three mornings running **and the feedback loop
records your marks**". This is the recording half; `brief/items.py` writes the rows and
`brief/read.py::read_feedback` reads them back into the next run's ranking.

A CLI verb rather than a form, because there is no web server anywhere in this architecture
and SPEC §4's diagram has no serving layer past Athena. Adding one to collect two bits a day
would be a component SPEC §14 would then have to justify.

## It checks before it writes

Athena's `UPDATE` reports no affected-row count, so a typo'd cluster id would otherwise
succeed silently and the reader would believe a mark was recorded that never was. This reads
the row first, refuses if it does not exist, and reads back what was stored afterwards —
"verified, not assumed", which is the habit this project applies to SNS subscriptions and SES
identities for the same reason.
"""

from __future__ import annotations

from typing import Any

from signal_core.brief.items import BRIEF_ITEMS_TABLE
from signal_core.config import Settings
from signal_core.ops.athena import AthenaQueryFailed, run_query
from signal_core.timeutil import brief_date

settings = Settings()

MARKS = ("up", "down", "clear")

# How much of a cluster id the brief prints and this command will accept. Cluster ids are
# 64-character content hashes; `run_list`'s docstring already conceded that "nobody is going
# to read one off the page", and the consequence showed up as a measurement in ADR-0015:
# `gold.brief_items` held **one** mark across 60 items, which is what refuses §14's
# weight-fitting re-entry. The gate was not waiting on the reader's diligence, it was waiting
# on a 64-character copy. Eight hex characters is 4 billion values against a ten-row brief.
SHORT_ID_CHARS = 8


def _resolve(
    prefix: str, day: str, table: str, database: str, workgroup: str, client: Any | None
) -> tuple[str | None, list[dict[str, Any]]]:
    """A full cluster id from what the reader typed, or None with the rows to show them.

    Refuses an ambiguous prefix rather than picking one. A wrong mark is worse than a retry:
    it feeds `ranker`'s feedback component, which is the one term that can subtract.
    """
    escaped = prefix.lower().replace("'", "''")
    matches = run_query(
        f"SELECT cluster_id, rank, title FROM {table} "
        f"WHERE brief_date = date '{day}' AND cluster_id LIKE '{escaped}%' "
        f"ORDER BY rank",
        database=database,
        workgroup=workgroup,
        client=client,
    )
    if len(matches.rows) == 1:
        return matches.rows[0]["cluster_id"], matches.rows
    return None, matches.rows


def run_feedback(
    date: str | None,
    cluster_id: str,
    mark: str,
    *,
    table: str = BRIEF_ITEMS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> int:
    """Record one mark. Returns a process exit code."""
    day = date or brief_date()
    mark = mark.strip().lower()
    if mark not in MARKS:
        print(f"unknown mark {mark!r}; expected one of {', '.join(MARKS)}")
        return 2

    database = database or settings.athena_database
    workgroup = workgroup or settings.athena_workgroup

    try:
        # A prefix is resolved to a full id first, so everything below still works on an
        # exact match and the ambiguous case is refused in one place.
        # `isalnum` is the test for "this looks like a truncated hash", not a safety check —
        # the query below escapes regardless. An id that fails it falls through to the exact
        # match, so a pathological cluster id still behaves exactly as it did before.
        if len(cluster_id) < 64 and cluster_id.isalnum():
            resolved, candidates = _resolve(cluster_id, day, table, database, workgroup, client)
            if resolved is None:
                if not candidates:
                    print(f"no item starting with {cluster_id!r} in the {day} brief.")
                else:
                    print(f"{cluster_id!r} is ambiguous — {len(candidates)} items match:")
                    for row in candidates:
                        short = (row.get("cluster_id") or "")[:SHORT_ID_CHARS]
                        print(f"  {short}  #{row.get('rank') or '?'}  {row.get('title') or ''}")
                print(f"`signal feedback --date {day} --list` shows what that brief contained.")
                return 1
            cluster_id = resolved
        escaped = cluster_id.replace("'", "''")

        existing = run_query(
            f"SELECT rank, title, user_feedback FROM {table} "
            f"WHERE brief_date = date '{day}' AND cluster_id = '{escaped}'",
            database=database,
            workgroup=workgroup,
            client=client,
        )
    except AthenaQueryFailed as failure:
        # The table not existing is the overwhelmingly likely cause on a first run, and it
        # has a specific fix, so it gets a specific message rather than a raw Athena error.
        print(f"could not read {table}: {failure}")
        print("has a brief been built yet? `make brief` writes the rows this updates.")
        return 1

    if not existing.rows:
        print(f"no item for cluster {cluster_id!r} in the {day} brief.")
        print(f"`signal feedback --date {day} --list` shows what that brief contained.")
        return 1

    row = existing.rows[0]
    value = "NULL" if mark == "clear" else f"'{mark}'"
    run_query(
        f"UPDATE {table} SET user_feedback = {value} "
        f"WHERE brief_date = date '{day}' AND cluster_id = '{escaped}'",
        database=database,
        workgroup=workgroup,
        client=client,
    )

    # Read back rather than trusting the UPDATE. See the module docstring.
    confirmed = run_query(
        f"SELECT user_feedback FROM {table} "
        f"WHERE brief_date = date '{day}' AND cluster_id = '{escaped}'",
        database=database,
        workgroup=workgroup,
        client=client,
    )
    stored = (confirmed.rows[0].get("user_feedback") if confirmed.rows else None) or "—"
    title = row.get("title") or cluster_id
    print(f"#{row.get('rank') or '?'} {title}")
    print(f"  {day}  {cluster_id[:SHORT_ID_CHARS]}  ->  {stored}")
    return 0


def run_list(
    date: str | None,
    *,
    table: str = BRIEF_ITEMS_TABLE,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> int:
    """What a given brief contained, with any marks already left.

    Exists because the cluster ids are content hashes: nobody is going to read one off the
    page and type it from memory, so the verb that needs one has to be able to show them.
    """
    day = date or brief_date()
    try:
        result = run_query(
            f"SELECT rank, cluster_id, title, score, user_feedback FROM {table} "
            f"WHERE brief_date = date '{day}' ORDER BY rank",
            database=database or settings.athena_database,
            workgroup=workgroup or settings.athena_workgroup,
            client=client,
        )
    except AthenaQueryFailed as failure:
        print(f"could not read {table}: {failure}")
        return 1

    if not result.rows:
        print(f"no brief items recorded for {day}.")
        return 1

    print(f"{day}  ({len(result.rows)} items)")
    for row in result.rows:
        mark = row.get("user_feedback") or "—"
        score = row.get("score") or ""
        print(
            f"  #{row.get('rank'):>2}  {mark:<5}  {row.get('cluster_id')}  {score:<8.8}  "
            f"{(row.get('title') or '')[:70]}"
        )
    print(f"\n{result.bytes_scanned:,} bytes scanned, ${result.cost_usd:.6f}")
    return 0
