"""Phase 4B DAG: the bitemporal macro load. SPEC §8, §9; docs/runbooks/phase-4b.md 4B.I.

Reads the `macro` partitions of the bronze window and MERGEs them into
`gold.macro_observations`, then recomputes `is_latest` and `revision_delta` across the whole
table. §8's differentiator: two time axes, so "what was knowable on 2026-03-14" is a query.

**Triggered by `MARKET_DAILY` — not by a cron, and still not by `BRONZE_COMMITTED`.** These
series release monthly, so booting a JVM on every hourly `BRONZE_COMMITTED` to re-merge
vintages that have not changed is 24x the work for one brief. That argument is unchanged and
is why this is not on the hourly asset. What changed is the other half of it: the old 03:40
cron only *happened* to land after `market` at 03:30, and it stopped happening the moment the
host slept through both and Airflow fired them in the same second. Following `market`'s asset
makes the order a fact rather than arithmetic the reader has to redo, at the same one run per
day. `BRONZE_COMMITTED` stays declared as an inlet for graph visibility.

The poll must have landed in staging and been committed to bronze before this can see it.
`market_dag`'s freshness gate is what guarantees that now, and it guarantees it once for the
whole chain instead of each DAG hoping separately.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import BRONZE_COMMITTED, MACRO_COMMITTED, MACRO_DAILY, MARKET_DAILY

# How far back to re-read. Wider than the daily cadence on purpose: the merge is insert-only
# on an immutable natural key, so re-reading a day already loaded costs a scan and commits
# nothing (SPEC §6.3's replay guarantee, in its easiest form). A day missed to an outage is
# therefore repaired by the next run rather than needing a manual backfill.
WINDOW_HOURS = 72


@dag(
    dag_id="macro",
    schedule=MARKET_DAILY,
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two loads recomputing `is_latest` at once would contend
    tags=["phase4b", "macro"],
)
def macro_dag():
    @task(inlets=[BRONZE_COMMITTED], outlets=[MACRO_COMMITTED, MACRO_DAILY])
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
