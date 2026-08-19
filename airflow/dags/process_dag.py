"""Phase 2 DAG: bronze -> silver. SPEC §7, §9, §12; docs/runbooks/phase-2.md 2.E.

Triggered by `BRONZE_COMMITTED` (`assets.py`), the Asset `ingest_monitor`'s
`commit_staged` task emits every time it merges the staged interval into
`bronze.raw_documents` — not a second cron guessing the commit already finished. Airflow
3's Asset scheduling means a run of this DAG starts right after bronze actually changes,
whether that is on the hour like `ingest_monitor`'s own schedule or not.

Two independent tasks, not one: `spark/jobs/normalize.py`'s "two Spark passes, one
schema each" decision (docs/runbooks/phase-2.md) means an article-parsing bug and a
comment-parsing bug can't take each other down, and both read only the bronze
partitions they need (`normalize_hn_comments_window` narrows to `source_id='hackernews'`
before either task opens a Spark session).
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import BRONZE_COMMITTED


@dag(
    dag_id="process",
    schedule=[BRONZE_COMMITTED],
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two runs MERGEing the same window would race each other
    tags=["phase2", "process"],
)
def process_dag():
    @task
    def normalize_articles() -> dict[str, int]:
        """bronze.raw_documents -> silver.articles + silver.parse_rejects, for the hour
        ingest_monitor's commit just closed."""
        from signal_core.ops.monitor import window_bounds
        from signal_core.spark.jobs.normalize import normalize_window
        from signal_core.spark.session import build_iceberg_session

        window_start, window_end = window_bounds()
        spark = build_iceberg_session("signal-normalize-articles")
        try:
            result = normalize_window(spark, window_start, window_end)
            return {
                "bronze_rows": result.bronze_rows,
                "skipped_rows": result.skipped_rows,
                "articles_committed": result.articles_committed,
                "articles_table_rows": result.articles_table_rows,
                "rejects_committed": result.rejects_committed,
                "rejects_table_rows": result.rejects_table_rows,
            }
        finally:
            spark.stop()

    @task
    def normalize_hn_comments() -> dict[str, int]:
        """bronze.raw_documents (hackernews partitions only) -> silver.hn_comments, same
        window as `normalize_articles` — reused via the same `window_bounds()` call, not
        recomputed, so the two tasks never quietly disagree on what "this run" covers."""
        from signal_core.ops.monitor import window_bounds
        from signal_core.spark.jobs.normalize import normalize_hn_comments_window
        from signal_core.spark.session import build_iceberg_session

        window_start, window_end = window_bounds()
        spark = build_iceberg_session("signal-normalize-hn-comments")
        try:
            result = normalize_hn_comments_window(spark, window_start, window_end)
            return {
                "hackernews_rows": result.hackernews_rows,
                "comments_extracted": result.comments_extracted,
                "comments_committed": result.comments_committed,
                "table_rows": result.table_rows,
            }
        finally:
            spark.stop()

    normalize_articles()
    normalize_hn_comments()


process_dag()
