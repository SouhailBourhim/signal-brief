"""Phase 3 DAG: silver.articles -> story clusters. SPEC §7.1, §12;
docs/runbooks/phase-3.md 3.B.

**Daily cron, not asset-triggered**, and that is the one interesting scheduling decision
here. `process` emits `SILVER_COMMITTED` every hour, but clustering reads a rolling 72-hour
window: recomputing it on every commit would do 24x the work for a product that is read
once a morning. The asset is still declared (`assets.py`) so the dependency is visible in
Airflow's graph rather than implied by a cron expression.

05:00 Africa/Casablanca puts the clusters in front of the reader before the brief is opened,
and inside the window where 4A's 16:00 mail has room to run.

Re-running is safe by construction: `cluster_window` replaces the window's partitions rather
than appending, so a manual trigger, a retry, or an overlapping backfill all converge on the
same rows. That property is why this DAG needs no catch-up logic of its own.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import CLUSTERS_COMMITTED, SILVER_COMMITTED

CLUSTER_WINDOW_HOURS = 72


@dag(
    dag_id="cluster",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 8, 20, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two runs overwriting the same partitions would race
    tags=["phase3", "cluster"],
)
def cluster_dag():
    @task(outlets=[CLUSTERS_COMMITTED], inlets=[SILVER_COMMITTED])
    def cluster_window_task() -> dict[str, int | str]:
        """The rolling 72-hour same-story window (SPEC §7.1), clustered and committed."""
        from datetime import timedelta

        from signal_core.spark.jobs.cluster import cluster_window
        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import utc_now

        until = utc_now()
        since = until - timedelta(hours=CLUSTER_WINDOW_HOURS)
        spark = build_iceberg_session("signal-cluster")
        try:
            result = cluster_window(spark, since, until)
            return {
                "articles_in": result.articles_in,
                "exact_duplicates_removed": result.exact_duplicates_removed,
                "candidate_pairs": result.candidate_pairs,
                "edges": result.edges,
                "clusters_out": result.clusters_out,
                # Reported, not logged: a run that quietly dissolved an oversized cluster
                # looks identical in the brief to one that never formed it (SPEC §11).
                "dissolved": result.dissolved,
                "dissolved_articles": result.dissolved_articles,
                "blocking_keys_dropped": result.blocking_keys_dropped,
                "ordering_key": result.ordering_key,
            }
        finally:
            spark.stop()

    cluster_window_task()


cluster_dag()
