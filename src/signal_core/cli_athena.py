"""`signal athena-query` — the acceptance test as a command, not a console screenshot.

SPEC §12's Phase 2 acceptance is "a stranger runs `make up`, answers an ad-hoc question in
Athena, and the bytes scanned and cost of that query are recorded." This is the "answers a
question" half: rows out, and the two numbers that make the record honest printed alongside
them, not buried in a log a stranger has to go find.
"""

from __future__ import annotations

from signal_core.config import settings
from signal_core.ops.athena import AthenaQueryFailed, run_query


def run_athena_query(sql: str, *, database: str | None = None, workgroup: str | None = None) -> int:
    try:
        result = run_query(
            sql,
            database=database or settings.athena_database,
            workgroup=workgroup or settings.athena_workgroup,
        )
    except AthenaQueryFailed as exc:
        print(f"query failed: {exc}")
        return 1
    except TimeoutError as exc:
        print(f"query timed out: {exc}")
        return 1

    for row in result.rows:
        print(row)
    mb_scanned = result.bytes_scanned / (1024**2)
    print(
        f"\n{len(result.rows)} rows — {mb_scanned:.2f} MB scanned — "
        f"${result.cost_usd:.6f} — {result.engine_execution_ms} ms engine time"
    )
    return 0
