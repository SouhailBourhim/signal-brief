"""Phase 1 DAG: commit what the pollers staged, then judge whether they are healthy.

SPEC §4 puts ingestion in AWS and processing local, so this DAG is the local half of the
loop: the Lambdas fetch on their own schedules and land bytes in S3, and hourly this
picks the staged interval up, merges it into `bronze.raw_documents`, and writes a verdict
per source into `ops.source_health`.

The order is deliberate. Committing first means the health assessment counts what is
actually in bronze rather than what a poller claimed to have staged — the two differ
exactly when something is broken, which is the moment the number matters.

Recovery is *planned* here and not executed: catch-up means re-fetching from a source,
which is the pollers' job and their rate limits, and SPEC §6.3's point is that most of it
is impossible anyway. What this DAG guarantees is that the impossible part is written
down as a `gap_reason` rather than left as an unexplained thin day.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# The deployed sources, which is `SOURCES` minus `fake` — the Phase 0 fixture source has
# no Lambda, no schedule, and no state item, so assessing it would report a permanent
# outage for something that was never running.
SOURCE_IDS = ["hackernews", "edgar", "rss_tech"]


@dag(
    dag_id="ingest_monitor",
    # Hourly, on the hour: the window this assesses is the hour that just closed.
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two runs merging the same staged objects would race the MERGE
    tags=["phase1", "ingest"],
)
def ingest_monitor_dag():
    @task
    def commit_staged() -> dict[str, int]:
        """Staging -> bronze.raw_documents. Idempotent, so a re-run is free."""
        from signal_core.config import settings
        from signal_core.spark.jobs.commit_bronze import commit
        from signal_core.spark.session import build_iceberg_session

        spark = build_iceberg_session("signal-commit-bronze")
        try:
            result = commit(spark, settings.bronze_staging_uri)
            return {
                "staged_rows": result.staged_rows,
                "committed_rows": result.committed_rows,
                "duplicate_rows": result.duplicate_rows,
                "table_rows": result.table_rows,
            }
        finally:
            spark.stop()

    @task
    def assess_sources(commit_stats: dict[str, int]) -> list[dict]:
        """One verdict per source, from DynamoDB state and the hour that just closed."""
        from signal_core.config import SOURCES, settings
        from signal_core.ops.monitor import assess, window_bounds
        from signal_core.spark.jobs.health_snapshot import record
        from signal_core.spark.session import build_iceberg_session
        from signal_core.state_store import DynamoDBStateStore

        del commit_stats  # dependency edge only — the counts below come from bronze
        window_start, window_end = window_bounds()
        store = DynamoDBStateStore(settings.state_table_name)

        spark = build_iceberg_session("signal-source-health")
        try:
            start = window_start.strftime("%Y-%m-%d %H:%M:%S")
            end = window_end.strftime("%Y-%m-%d %H:%M:%S")
            rows = spark.sql(
                f"""
                SELECT source_id, count(*) AS n
                FROM bronze.raw_documents
                WHERE fetched_at >= TIMESTAMP '{start}' AND fetched_at < TIMESTAMP '{end}'
                GROUP BY source_id
                """
            ).collect()
            counts = {row.source_id: row.n for row in rows}

            verdicts = [
                assess(SOURCES[source_id], store.load(source_id), counts.get(source_id, 0))
                for source_id in SOURCE_IDS
            ]
            record(spark, [v.health for v in verdicts], window_start)
        finally:
            spark.stop()

        return [
            {
                "source_id": v.health.source_id,
                "status": v.health.status,
                "docs": v.health.docs_ingested,
                "gap_reason": v.health.gap_reason,
                "catch_up_from": (
                    v.catch_up.recoverable_from.isoformat() if v.needs_catch_up else None
                ),
            }
            for v in verdicts
        ]

    @task
    def raise_on_degraded(verdicts: list[dict]) -> None:
        """Fail the run when a source is stale or gapped.

        A task that always succeeds is not monitoring. Failing here is what puts the
        source in Airflow's own alerting path — the CloudWatch alarms in
        infra/terraform/main only see whether the Lambda *ran*, which a dead-but-200 feed
        (SPEC §11) passes with full marks.
        """
        degraded = [v for v in verdicts if v["status"] in {"stale", "never_succeeded", "gapped"}]
        if degraded:
            raise AirflowFailException(f"degraded sources: {degraded}")

    raise_on_degraded(assess_sources(commit_staged()))


ingest_monitor_dag()
