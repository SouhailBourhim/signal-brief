"""Phase 4A DAG: nightly Iceberg maintenance. SPEC §11, §12; docs/runbooks/phase-4a.md 4A.F.

Compaction, snapshot expiry and orphan cleanup across every table this pipeline writes, with
before/after file counts recorded to `ops.maintenance_runs` — SPEC §12's 4A acceptance asks
for a measured compaction delta, and this is where the number comes from.

**Cron at 02:00, not asset-triggered.** Iceberg's snapshot isolation means compaction is safe
to run alongside readers and writers regardless of timing, so the schedule is about resource
contention rather than correctness: 02:00 Africa/Casablanca is off-peak and clear of the
02:11 market poll, the 03:30 market DAG, `resolve` at 04:30, `cluster` at 05:00 and the 16:00
brief. `SILVER_COMMITTED` is declared as an inlet for graph visibility only — `assets.py`
already anticipated this ("so 4A's maintenance DAG has something to hang off").

One task over all tables rather than one per table, matching `cluster_dag`'s preference for
fewer moving parts. The job itself never raises per table (see `maintain_table`); this DAG
fails on the aggregate, so one locked table is visible without abandoning the other eleven.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from alerting import DEFAULT_ARGS, on_dag_success
from assets import SILVER_COMMITTED


@dag(
    dag_id="maintenance",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two sweeps rewriting the same files would contend
    default_args=DEFAULT_ARGS,
    on_success_callback=on_dag_success,
    tags=["phase4a", "maintenance"],
)
def maintenance_dag():
    @task(inlets=[SILVER_COMMITTED])
    def sweep() -> dict[str, int | str]:
        from signal_core.spark.jobs.maintain import maintain
        from signal_core.spark.session import build_iceberg_session

        spark = build_iceberg_session("signal-maintenance")
        try:
            result = maintain(spark)
            for table in result.tables:
                note = f" ERROR {table.error}" if table.error else ""
                if table.skipped:
                    note += f" SKIPPED {table.skipped}"
                print(
                    f"{table.table}: {table.files_before} -> {table.files_after} files "
                    f"(delta {table.delta}, {table.rewritten_bytes:,} bytes rewritten){note}"
                )

            if result.skipped:
                # Printed, never raised. Orphan removal is unavailable against the deployed
                # S3 warehouse by a documented decision (`maintain._NO_HADOOP_FS`), so
                # failing here would mean a red DAG every night for a known limitation —
                # and a task that is always red is one nobody reads. It stays visible in
                # `ops.maintenance_runs` and in this line.
                print(f"skipped on {len(result.skipped)} tables: {result.tables[0].skipped}")

            if result.failed:
                # Loud rather than logged. A maintenance job that quietly stops working
                # shows up months later as slow queries and a storage bill, which is exactly
                # the silent-failure class SPEC §11 exists to catch.
                raise RuntimeError(f"maintenance failed for: {', '.join(result.failed)}")

            return {
                "tables": len(result.tables),
                "files_before": result.files_before,
                "files_after": result.files_after,
                "delta": result.delta,
            }
        finally:
            spark.stop()

    sweep()


maintenance_dag()
