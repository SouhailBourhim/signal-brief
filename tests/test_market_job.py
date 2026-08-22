"""`silver.market_observations`. SPEC §7.4; ADR-0010.

The one silver table whose MERGE updates on match, because its source restates history.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import write_staging
from signal_core.timeutil import utc_now

pytestmark = pytest.mark.spark

BRONZE_TABLE = "bronze.raw_documents"
MARKET_TABLE = "silver.market_observations"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-market", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture
def staging(tmp_path):
    return tmp_path / "staging"


@pytest.fixture(autouse=True)
def clean_tables(spark):
    """Each test starts from empty tables — the module-scoped session is shared, the data
    is not."""
    for table in (BRONZE_TABLE, MARKET_TABLE, "silver.articles", "silver.parse_rejects"):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    yield


def _chart(ticker: str, bars: list[tuple[int, float]]) -> bytes:
    """`bars` is [(epoch_seconds, close), ...]."""
    return json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {"symbol": ticker},
                        "timestamp": [b[0] for b in bars],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [b[1] for b in bars],
                                    "high": [b[1] for b in bars],
                                    "low": [b[1] for b in bars],
                                    "close": [b[1] for b in bars],
                                    "volume": [100.0 for _ in bars],
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }
    ).encode()


def _doc(index: int, payload: bytes, *, fetched_at: datetime | None = None) -> RawDocument:
    return RawDocument(
        ingest_id=f"market-{index:04d}",
        source_id="market",
        fetched_at=fetched_at or utc_now(),
        source_url=f"https://example.test/market/{index}",
        http_status=200,
        outcome=FetchOutcome.OK,
        etag=None,
        last_modified=None,
        content_hash=f"{index:064d}",
        payload=payload,
        payload_format=PayloadFormat.JSON,
        latency_ms=12,
        byte_count=len(payload),
    )


def _commit(spark, staging, docs) -> None:
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging(docs, staging)
    commit(spark, staging, table=BRONZE_TABLE)


def _window(fetched_at: datetime) -> tuple[datetime, datetime]:
    return fetched_at - timedelta(minutes=1), fetched_at + timedelta(minutes=1)


def test_bars_commit_one_row_per_ticker_and_date(spark, staging):
    from signal_core.spark.jobs.market import market_window

    now = utc_now()
    _commit(spark, staging, [_doc(1, _chart("AAPL", [(1786060800, 1.5), (1786147200, 2.0)]))])

    result = market_window(spark, *_window(now))

    assert result.market_rows == 1, "one bronze document"
    assert result.observations_committed == 2, "carrying two daily bars"
    rows = spark.table(MARKET_TABLE).collect()
    assert {r.ticker for r in rows} == {"AAPL"}
    assert len({r.trade_date for r in rows}) == 2


def test_a_restated_bar_overwrites_rather_than_duplicating(spark, staging):
    """The reason this table's MERGE differs from its siblings'. A split restates every
    prior bar, and the restated number is the correct one — `silver.articles`' insert-only
    rule would leave pre-split prices describing nothing."""
    from signal_core.spark.jobs.market import market_window

    first_at = utc_now()
    _commit(spark, staging, [_doc(1, _chart("AAPL", [(1786060800, 100.0)]), fetched_at=first_at)])
    market_window(spark, *_window(first_at))

    later = first_at + timedelta(days=1)
    _commit(spark, staging, [_doc(2, _chart("AAPL", [(1786060800, 25.0)]), fetched_at=later)])
    result = market_window(spark, *_window(later))

    rows = spark.table(MARKET_TABLE).collect()
    assert len(rows) == 1, "one bar for one (ticker, trade_date), not two"
    assert rows[0].close == 25.0, "the restatement wins"
    assert result.observations_committed == 0, "an update is not a new row"


def test_replay_converges_rather_than_duplicating(spark, staging):
    """SPEC §6.3: replay is deterministic. Re-running the same window must not grow the
    table."""
    from signal_core.spark.jobs.market import market_window

    now = utc_now()
    _commit(spark, staging, [_doc(1, _chart("NVDA", [(1786060800, 3.0), (1786147200, 4.0)]))])

    first = market_window(spark, *_window(now))
    replay = market_window(spark, *_window(now))

    assert first.observations_committed == 2
    assert replay.observations_committed == 0
    assert spark.table(MARKET_TABLE).count() == 2


def test_overlapping_fetches_in_one_window_keep_the_newest_observation(spark, staging):
    """Every pair of consecutive fetches overlaps by ~62 of 63 bars, so this is the normal
    case rather than an edge one. The newest observation is the one carrying a restatement."""
    from signal_core.spark.jobs.market import market_window

    now = utc_now()
    earlier = now - timedelta(seconds=30)
    _commit(
        spark,
        staging,
        [
            _doc(1, _chart("AAPL", [(1786060800, 10.0)]), fetched_at=earlier),
            _doc(2, _chart("AAPL", [(1786060800, 12.0)]), fetched_at=now),
        ],
    )

    market_window(spark, *_window(now))

    rows = spark.table(MARKET_TABLE).collect()
    assert len(rows) == 1
    assert rows[0].close == 12.0, "the later fetch wins within one window too"


def test_an_unknown_ticker_response_commits_nothing(spark, staging):
    """Yahoo answers a delisted symbol in-band with HTTP 200; it must not look like a
    ticker that simply had no trading days."""
    from signal_core.spark.jobs.market import market_window

    now = utc_now()
    payload = json.dumps(
        {"chart": {"result": None, "error": {"code": "Not Found", "description": "delisted"}}}
    ).encode()
    _commit(spark, staging, [_doc(1, payload)])

    result = market_window(spark, *_window(now))

    assert result.market_rows == 1
    assert result.observations_committed == 0
    assert spark.table(MARKET_TABLE).count() == 0


def test_market_rows_stay_out_of_silver_articles(spark, staging):
    """`NON_ARTICLE_SOURCES`: a price bar is not an article, and counting these in the
    articles pass would show a rising bronze count against a flat article count."""
    from signal_core.spark.jobs.normalize import normalize_window

    now = utc_now()
    _commit(spark, staging, [_doc(1, _chart("AAPL", [(1786060800, 1.0)]))])

    result = normalize_window(spark, *_window(now))

    assert result.bronze_rows == 0, "excluded from the pass entirely, not parsed to nothing"
    assert result.articles_committed == 0
