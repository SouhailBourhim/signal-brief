"""Phase 0 DAG: runs the walking skeleton on a schedule.

Placeholder in the sense that it does one thing, but real in the sense that it proves
Airflow can import the package, reach the data volume, and produce a brief. Phases 1-4
add ingest_*, process, enrich, macro, quality, maintenance, and backfill DAGs alongside it.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="skeleton",
    # 06:30 local so the brief exists before the 16:00 read (SPEC §1).
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Casablanca"),
    catchup=False,
    tags=["phase0"],
)
def skeleton_dag():
    @task
    def run_skeleton() -> str:
        from signal_core.skeleton import run

        return str(run())

    run_skeleton()


skeleton_dag()
