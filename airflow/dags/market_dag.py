"""Phase 4A DAG: bronze `market` partitions -> silver.market_observations. SPEC §7.4;
ADR-0010.

**Daily cron, and the head of the daily chain.** The `market` poller fires once a day, so
`BRONZE_COMMITTED` — which `ingest_monitor` emits every hour — would trigger this 24 times to
find new bars once, and each of those runs boots a JVM. So this stays on a clock, while the
stages *after* it hang off `MARKET_DAILY` (`assets.py`) instead of their own crons: one run a
day each, in a fixed order, rather than five cron expressions that only look ordered because
they are read together.

03:30 Africa/Casablanca still sits after the poller's 02:11 UTC fetch. What changed is that
`macro`, `resolve`, `cluster` and `enrich` now follow this run rather than the clock.

**The gate is the whole point of being the head.** A cron only means what it says if the
machine is awake to see it. On 2026-08-29 the host resumed after ~16 hours asleep and Airflow
fired every overdue cron in the same second: `market`, `macro`, `resolve`, `cluster` and
`enrich` all started at 13:24:14 and were finished within two minutes — while `ingest_monitor`
was still committing the night's staged bytes into bronze. All five read pre-sleep data and
reported success. Ordering the chain fixes them relative to *each other*; it does nothing about
the commit they all race, because being first is worthless if first still means "before bronze
exists". So this DAG blocks until bronze has been committed recently, and everything
downstream inherits that wait by construction.

Failing the gate halts the chain, which is deliberate: a brief built on yesterday's bronze is
the failure this is here to prevent, and it is indistinguishable from a good one once sent.
The cost of the same property is that a `market` failure now stops `cluster` too — loud and
visible, rather than five DAGs quietly disagreeing about what day it is.

Re-running is safe by construction: `market_window` MERGEs on `(ticker, trade_date)` and
updates on match, so a retry or an overlapping window converges rather than duplicating.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import BRONZE_COMMITTED, MARKET_DAILY, SILVER_COMMITTED

# Wide enough to repair a gap without a backfill: every fetch re-states ~63 trading days,
# so a window covering the last few days of *fetches* recovers any bar a missed run dropped.
MARKET_WINDOW_HOURS = 72

# How stale bronze may be before the chain refuses to start. `ingest_monitor` commits hourly,
# so two hours is one missed slot of slack — tight enough that a 16-hour sleep is caught,
# loose enough that a single slow commit is not treated as an outage.
BRONZE_MAX_AGE_HOURS = 2

# Poked in `reschedule` mode, so the wait costs a metadata query every 5 minutes rather than a
# worker slot held for hours. The timeout is what turns "bronze never came back" into a
# failure instead of a DAG that waits forever and is discovered by its absence.
GATE_POKE_SECONDS = 300
GATE_TIMEOUT_SECONDS = 4 * 60 * 60


@dag(
    dag_id="market",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two runs MERGEing the same keys would race
    tags=["phase4a", "market"],
)
def market_dag():
    @task.sensor(
        poke_interval=GATE_POKE_SECONDS,
        timeout=GATE_TIMEOUT_SECONDS,
        mode="reschedule",
    )
    def wait_for_fresh_bronze() -> bool:
        """Block until `ingest_monitor` has committed to bronze recently.

        Reads Iceberg's `snapshots` metadata table rather than scanning the data, so a poke
        costs a metadata read and not a table scan. `max(fetched_at)` over the rows would
        answer a different question anyway — how new the *documents* are, which a source that
        stopped publishing also makes old. What this needs to know is whether the commit has
        happened, and that is a property of the table, not of its contents.
        """
        from datetime import timedelta

        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import ensure_utc, utc_now

        cutoff = utc_now() - timedelta(hours=BRONZE_MAX_AGE_HOURS)
        spark = build_iceberg_session("signal-bronze-freshness")
        try:
            rows = spark.sql(
                "SELECT max(committed_at) AS last_commit FROM bronze.raw_documents.snapshots"
            ).collect()
            last_commit = rows[0].last_commit if rows else None
        except Exception as exc:
            # On a fresh deployment bronze does not exist until the first commit. That is the
            # same state as "the commit has not happened yet" from here, so it waits rather
            # than failing — and the sensor timeout is what stops it waiting forever.
            print(f"bronze.raw_documents not readable yet ({exc.__class__.__name__}) — waiting")
            return False
        finally:
            spark.stop()

        if last_commit is None:
            print("bronze.raw_documents has no snapshots yet — waiting")
            return False

        last_commit = ensure_utc(last_commit)
        fresh = last_commit >= cutoff
        print(
            f"bronze last committed {last_commit.isoformat()}; "
            f"cutoff {cutoff.isoformat()} ({BRONZE_MAX_AGE_HOURS}h) -> "
            f"{'proceeding' if fresh else 'waiting'}"
        )
        return fresh

    @task(outlets=[SILVER_COMMITTED, MARKET_DAILY], inlets=[BRONZE_COMMITTED])
    def market_window_task() -> dict[str, int]:
        """Daily OHLCV bars for the watchlist's tickers, committed for §7.4's
        market-corroboration component."""
        from datetime import timedelta

        from signal_core.spark.jobs.market import market_window
        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import utc_now

        until = utc_now()
        since = until - timedelta(hours=MARKET_WINDOW_HOURS)
        spark = build_iceberg_session("signal-market")
        try:
            result = market_window(spark, since, until)
            return {
                "market_rows": result.market_rows,
                "observations_extracted": result.observations_extracted,
                # Net new rows. A day where every bar is a restatement of one already held
                # commits 0 and is not the same thing as a day that fetched nothing —
                # `market_rows` is what separates them (SPEC §11).
                "observations_committed": result.observations_committed,
                "table_rows": result.table_rows,
            }
        finally:
            spark.stop()

    wait_for_fresh_bronze() >> market_window_task()


market_dag()
