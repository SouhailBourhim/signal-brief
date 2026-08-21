"""Phase 3 DAG: silver.articles -> entity mentions, plus the SCD2 entity dimension.
SPEC §7.2, §9, §12; docs/runbooks/phase-3.md 3.C.

**Daily cron at 04:30, half an hour ahead of `cluster`.** Same reasoning as that DAG — the
product is read once a morning, so recomputing on every hourly commit is work nobody sees —
and the offset is so that 3.D's brief, which joins clusters to entities, finds both tables
rebuilt from the same day's silver rather than one of them from yesterday.

The dimension loads before the mentions, and the order is deliberate even though nothing
enforces a foreign key: a mention resolved to an entity that `dim_entities` has never heard
of is a broken join waiting to be found by a reader instead of by this DAG.

Re-running is safe by construction, in two different ways worth naming because they are not
the same mechanism:

- `resolve_window` **replaces** the day partitions it touches. A mention is a function of
  (article, dictionary, algorithm), not a fact, so a re-run converges.
- `load_entities` is **idempotent by comparison**: loading an unchanged snapshot supersedes
  nothing, because nothing tracked differs. It only ever appends history when the dictionary
  actually changed, which is what makes a non-zero `superseded` worth looking at.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import MENTIONS_RESOLVED, SILVER_COMMITTED

RESOLVE_WINDOW_HOURS = 72


@dag(
    dag_id="resolve",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2026, 8, 21, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two runs overwriting the same partitions would race
    tags=["phase3", "entities"],
)
def resolve_dag():
    @task
    def load_entities_task() -> dict[str, int | str]:
        """The committed dictionary snapshot, loaded into `dim_entities` as SCD2."""
        from signal_core.spark.jobs.resolve import load_entities
        from signal_core.spark.session import build_iceberg_session

        spark = build_iceberg_session("signal-load-entities")
        try:
            result = load_entities(spark)
            return {
                "snapshot_entities": result.snapshot_entities,
                "inserted": result.inserted,
                # Companies that renamed. Reported rather than logged: a dimension that
                # silently rewrote a name would make every historical mention wrong and
                # look like nothing happened.
                "superseded": result.superseded,
                "unchanged": result.unchanged,
                "valid_from": result.valid_from.isoformat(),
            }
        finally:
            spark.stop()

    @task(outlets=[MENTIONS_RESOLVED], inlets=[SILVER_COMMITTED])
    def resolve_window_task() -> dict[str, int | float | str]:
        """The rolling window's articles, resolved and committed."""
        from datetime import timedelta

        from signal_core.spark.jobs.resolve import resolve_window
        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import utc_now

        until = utc_now()
        since = until - timedelta(hours=RESOLVE_WINDOW_HOURS)
        spark = build_iceberg_session("signal-resolve")
        try:
            result = resolve_window(spark, since, until)
            return {
                "articles_in": result.articles_in,
                "mentions_detected": result.mentions_detected,
                "mentions_linked": result.mentions_linked,
                "distinct_entities": result.distinct_entities,
                # The shape of the abstentions, not just the count. SPEC §7.2's floor means
                # a broken dictionary shows up here as a surge in `no-such-entity` rather
                # than as an error, and this is the only place that becomes visible.
                "link_rate": round(result.link_rate, 4),
                "dictionary_built_at": result.dictionary_built_at,
                **{f"unlinked_{reason}": n for reason, n in result.by_reason.items()},
            }
        finally:
            spark.stop()

    load_entities_task() >> resolve_window_task()


resolve_dag()
