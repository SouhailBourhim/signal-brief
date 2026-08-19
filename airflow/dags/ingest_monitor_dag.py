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
from assets import BRONZE_COMMITTED


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
    # `outlets`: this is the only task that ever changes bronze.raw_documents, so it's
    # what `process_dag` (2.E) triggers off of — an Asset event, not a second cron
    # hoping the commit already finished (docs/runbooks/phase-2.md 2.E).
    @task(outlets=[BRONZE_COMMITTED])
    def commit_staged() -> dict[str, int]:
        """Staging -> local cache -> bronze.raw_documents. Idempotent at both steps, so
        a re-run costs neither egress nor duplicate rows."""
        from airflow.sdk import get_current_context

        from signal_core.config import settings
        from signal_core.spark.jobs.commit_bronze import commit
        from signal_core.spark.jobs.cost_snapshot import CostRecord
        from signal_core.spark.jobs.cost_snapshot import record as record_costs
        from signal_core.spark.session import build_iceberg_session
        from signal_core.staging import sync_staging
        from signal_core.timeutil import utc_now

        sync = sync_staging(settings.bronze_staging_uri, settings.staging_cache_root)

        spark = build_iceberg_session("signal-commit-bronze")
        try:
            result = commit(spark, sync.local_root)
            # SPEC §10.1: egress is the line item nobody budgets. `sync` already
            # measures it — 2.D's fix is writing it down instead of letting it die once
            # this task returns, which is what happened before `ops.pipeline_costs`
            # existed to hold it.
            record_costs(
                spark,
                [
                    CostRecord(
                        run_id=get_current_context()["run_id"],
                        dag_id="ingest_monitor",
                        task_id="commit_staged",
                        run_date=utc_now().date(),
                        s3_egress_bytes=sync.bytes_downloaded,
                        # `s3_requests` stays unset: `sync.objects` counts objects
                        # considered, not API calls made (skipped objects cost a local
                        # stat, not a request), and SyncResult doesn't track the latter.
                        # SPEC §17 — claim it once it's actually measured, not before.
                    )
                ],
            )
            return {
                "objects_seen": sync.objects,
                "objects_downloaded": sync.downloaded,
                "egress_bytes": sync.bytes_downloaded,
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
        from signal_core.config import DEPLOYED_SOURCE_IDS, SOURCES, settings
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
                for source_id in DEPLOYED_SOURCE_IDS
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
