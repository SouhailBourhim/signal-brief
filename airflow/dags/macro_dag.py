"""Phase 4B DAG: the bitemporal macro load. SPEC §8, §9; docs/runbooks/phase-4b.md 4B.I.

Reads the `macro` partitions of the bronze window and MERGEs them into
`gold.macro_observations`, then recomputes `is_latest` and `revision_delta` across the whole
table. §8's differentiator: two time axes, so "what was knowable on 2026-03-14" is a query.

**Its own daily cron, not asset-triggered**, for exactly the reason `market_dag` has one:
these series release monthly, and booting a JVM on every hourly `BRONZE_COMMITTED` to
re-merge vintages that have not changed is 24x the work for one brief. `BRONZE_COMMITTED` is
declared as an inlet for graph visibility.

**03:40, after the 02:26 poll and before the 05:00 cluster run.** The poll needs to have
landed in staging and been committed to bronze by `ingest_monitor` before this can see it.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import BRONZE_COMMITTED, MACRO_COMMITTED

# How far back to re-read. Wider than the daily cadence on purpose: the merge is insert-only
# on an immutable natural key, so re-reading a day already loaded costs a scan and commits
# nothing (SPEC §6.3's replay guarantee, in its easiest form). A day missed to an outage is
# therefore repaired by the next run rather than needing a manual backfill.
WINDOW_HOURS = 72


@dag(
    dag_id="macro",
    schedule="40 3 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two loads recomputing `is_latest` at once would contend
    tags=["phase4b", "macro"],
)
def macro_dag():
    @task(inlets=[BRONZE_COMMITTED], outlets=[MACRO_COMMITTED])
    def load() -> dict[str, int]:
        from datetime import timedelta

        from signal_core.spark.jobs.macro import macro_window
        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import utc_now

        until = utc_now()
        since = until - timedelta(hours=WINDOW_HOURS)

        spark = build_iceberg_session("signal-macro")
        try:
            result = macro_window(spark, since, until)
            print(
                f"{result.bronze_rows} bronze rows, {result.series_seen} series, "
                f"{result.observations_extracted} observations extracted, "
                f"{result.observations_committed} newly committed, "
                f"{result.revisions_found} revisions, {result.table_rows} rows in table"
            )
            if result.bronze_rows and not result.observations_extracted:
                # Bronze had macro documents and none of them yielded an observation. That is
                # a parse or a key failure, not a quiet month — and the difference matters,
                # because a genuinely quiet month still yields the full re-stated history.
                raise RuntimeError(
                    f"{result.bronze_rows} macro documents produced 0 observations — "
                    "check silver.parse_rejects and the FRED key (macro.tf)"
                )
            return {
                "bronze_rows": result.bronze_rows,
                "observations_committed": result.observations_committed,
                "revisions_found": result.revisions_found,
                "table_rows": result.table_rows,
            }
        finally:
            spark.stop()

    load()


macro_dag()
