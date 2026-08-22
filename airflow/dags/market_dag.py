"""Phase 4A DAG: bronze `market` partitions -> silver.market_observations. SPEC §7.4;
ADR-0010.

**Daily cron, not asset-triggered**, and for a sharper version of `cluster_dag`'s reason.
The `market` poller fires once a day, so `BRONZE_COMMITTED` — which `ingest_monitor` emits
every hour — would trigger this 24 times to find new bars once. Each of those runs boots a
JVM. The asset is declared as an inlet so the dependency shows in Airflow's graph rather
than being implied by two cron expressions that have to be read together.

03:30 Africa/Casablanca sits after the poller's 02:11 UTC fetch and before `resolve` (04:30)
and `cluster` (05:00), so the bars are committed by the time anything reads them.

Re-running is safe by construction: `market_window` MERGEs on `(ticker, trade_date)` and
updates on match, so a retry or an overlapping window converges rather than duplicating.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import BRONZE_COMMITTED, SILVER_COMMITTED

# Wide enough to repair a gap without a backfill: every fetch re-states ~63 trading days,
# so a window covering the last few days of *fetches* recovers any bar a missed run dropped.
MARKET_WINDOW_HOURS = 72


@dag(
    dag_id="market",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two runs MERGEing the same keys would race
    tags=["phase4a", "market"],
)
def market_dag():
    @task(outlets=[SILVER_COMMITTED], inlets=[BRONZE_COMMITTED])
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

    market_window_task()


market_dag()
