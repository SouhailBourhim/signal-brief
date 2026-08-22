from __future__ import annotations

import httpx
import pytest
import respx

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, State
from signal_core.sources import market
from signal_core.watchlist import Watchlist


@pytest.fixture
def config():
    return SOURCES["market"]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(market.time, "sleep", lambda _seconds: None)


@pytest.fixture
def watchlist(monkeypatch):
    """The poller reads the watchlist at fetch time, so tests pin it rather than depending
    on whatever the committed file happens to hold."""

    def _set(*tickers: str) -> None:
        monkeypatch.setattr(
            market,
            "load_watchlist",
            lambda: Watchlist(companies=frozenset(tickers), technologies=(), macro_series=()),
        )

    return _set


def _chart(ticker: str) -> dict:
    return {
        "chart": {
            "result": [
                {
                    "meta": {"symbol": ticker},
                    "timestamp": [1786060800],
                    "indicators": {
                        "quote": [
                            {
                                "open": [1.0],
                                "high": [2.0],
                                "low": [0.5],
                                "close": [1.5],
                                "volume": [100],
                            }
                        ]
                    },
                }
            ],
            "error": None,
        }
    }


@respx.mock
def test_it_fetches_one_document_per_watchlist_ticker(config, watchlist):
    watchlist("AAPL", "NVDA")
    for ticker in ("AAPL", "NVDA"):
        respx.get(f"{config.url}/v8/finance/chart/{ticker}").mock(
            return_value=httpx.Response(200, json=_chart(ticker))
        )

    documents, state = market.poll(config, State(source_id=config.source_id))

    assert len(documents) == 2
    assert all(d.outcome == FetchOutcome.OK for d in documents)
    assert state.consecutive_failures == 0


@respx.mock
def test_lowercase_watchlist_entries_are_not_fetched(config, watchlist):
    """`entities/dictionary.py`'s namespace: UPPERCASE is a tradable ticker, lower-kebab-case
    is an entity without one. A private company counts for relevance and has no price."""
    watchlist("AAPL", "openai")
    respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(
        return_value=httpx.Response(200, json=_chart("AAPL"))
    )

    documents, _ = market.poll(config, State(source_id=config.source_id))

    assert len(documents) == 1
    assert "AAPL" in documents[0].source_url


@respx.mock
def test_the_request_asks_for_a_window_not_a_single_bar(config, watchlist):
    """One fetch has to carry its own baseline: the corroboration threshold compares the
    latest return against the trailing window's stddev, and both come from this response.
    Fetching one bar would mean twenty days before the component could say anything."""
    watchlist("AAPL")
    route = respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(
        return_value=httpx.Response(200, json=_chart("AAPL"))
    )

    market.poll(config, State(source_id=config.source_id))

    request_url = str(route.calls[0].request.url)
    assert "range=3mo" in request_url
    assert "interval=1d" in request_url


@respx.mock
def test_an_empty_watchlist_is_not_a_failure(config, watchlist):
    """A configuration statement, not an outage. Reporting it as an error would page
    someone about a file they meant to empty."""
    watchlist()

    documents, state = market.poll(config, State(source_id=config.source_id))

    assert documents == []
    assert state.consecutive_failures == 0
    assert state.last_success_at is not None


@respx.mock
def test_one_failing_ticker_does_not_cost_the_others(config, watchlist):
    """SPEC §6.2: a failed fetch becomes an ERROR document, never an escaped exception."""
    watchlist("AAPL", "NVDA")
    respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(return_value=httpx.Response(500))
    respx.get(f"{config.url}/v8/finance/chart/NVDA").mock(
        return_value=httpx.Response(200, json=_chart("NVDA"))
    )

    documents, state = market.poll(config, State(source_id=config.source_id))

    assert [d.outcome for d in documents] == [FetchOutcome.ERROR, FetchOutcome.OK]
    assert state.consecutive_failures == 0


@respx.mock
def test_unchanged_prices_are_content_staleness_not_fetch_staleness(config, watchlist):
    """SPEC §11 and 1.E: a source can succeed and still be dead. Over a weekend the same
    bars come back with a fresh envelope, and only the payload hash can see that."""
    watchlist("AAPL")
    respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(
        return_value=httpx.Response(200, json=_chart("AAPL"))
    )

    _, first = market.poll(config, State(source_id=config.source_id))
    _, second = market.poll(config, first)

    assert second.last_content_change_at == first.last_content_change_at
    assert second.last_success_at > first.last_success_at


@respx.mock
def test_a_new_bar_registers_as_content_movement(config, watchlist):
    watchlist("AAPL")
    respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(
        return_value=httpx.Response(200, json=_chart("AAPL"))
    )
    _, first = market.poll(config, State(source_id=config.source_id))

    moved = _chart("AAPL")
    moved["chart"]["result"][0]["indicators"]["quote"][0]["close"] = [99.0]
    respx.get(f"{config.url}/v8/finance/chart/AAPL").mock(
        return_value=httpx.Response(200, json=moved)
    )
    _, second = market.poll(config, first)

    assert second.last_content_change_at > first.last_content_change_at
