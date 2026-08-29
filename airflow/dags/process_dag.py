"""Phase 2 DAG: bronze -> silver. SPEC §7, §9, §12; docs/runbooks/phase-2.md 2.E.

Triggered by `BRONZE_COMMITTED` (`assets.py`), the Asset `ingest_monitor`'s
`commit_staged` task emits every time it merges the staged interval into
`bronze.raw_documents` — not a second cron guessing the commit already finished. Airflow
3's Asset scheduling means a run of this DAG starts right after bronze actually changes,
whether that is on the hour like `ingest_monitor`'s own schedule or not.

Three independent tasks, not one: `spark/jobs/normalize.py`'s "one Spark pass, one schema
each" decision (docs/runbooks/phase-2.md) means an article-parsing bug, a comment-parsing
bug and a score-parsing bug can't take each other down, and each reads only the bronze
partitions it needs (`normalize_hn_comments_window` narrows to `source_id='hackernews'`,
`normalize_hn_scores_window` to `'hn_scores'`) before opening a Spark session.

The third arrived in Phase 4A with SPEC §7.4's velocity component. Note that the articles
pass now *excludes* `hn_scores` rather than parsing them to nothing — see
`normalize.NON_ARTICLE_SOURCES`.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from alerting import DEFAULT_ARGS, on_dag_success
from assets import BRONZE_COMMITTED, SILVER_COMMITTED


@dag(
    dag_id="process",
    schedule=[BRONZE_COMMITTED],
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two runs MERGEing the same window would race each other
    default_args=DEFAULT_ARGS,
    on_success_callback=on_dag_success,
    tags=["phase2", "process"],
)
def process_dag():
    @task(outlets=[SILVER_COMMITTED])
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

    @task
    def normalize_hn_scores() -> dict[str, int]:
        """bronze.raw_documents (hn_scores partitions only) -> silver.hn_score_snapshots.

        Phase 4A, SPEC §7.4's velocity component. Third independent task for the same
        reason there are two: one output schema per pass, and a failure here cannot take
        the articles down. Same `window_bounds()` call as its siblings."""
        from signal_core.ops.monitor import window_bounds
        from signal_core.spark.jobs.normalize import normalize_hn_scores_window
        from signal_core.spark.session import build_iceberg_session

        window_start, window_end = window_bounds()
        spark = build_iceberg_session("signal-normalize-hn-scores")
        try:
            result = normalize_hn_scores_window(spark, window_start, window_end)
            return {
                "hn_scores_rows": result.hn_scores_rows,
                "snapshots_extracted": result.snapshots_extracted,
                "snapshots_committed": result.snapshots_committed,
                "table_rows": result.table_rows,
            }
        finally:
            spark.stop()

    normalize_articles()
    normalize_hn_comments()
    normalize_hn_scores()


process_dag()
