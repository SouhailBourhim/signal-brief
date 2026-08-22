"""Daily OHLCV bars for the watchlist's tickers. SPEC §7.4's market-corroboration input.

## No client library, and no CSV

ADR-0010 records the full trail. Short version: `yfinance` is unavailable because it pulls
pandas transitively and `tests/test_lambda_artifact.py` forbids that in the handler's import
chain (ADR-0006). Stooq's CSV endpoint was chosen instead and then found to answer every
request — any User-Agent — with a JavaScript proof-of-work challenge, which a poller cannot
clear and should not try to. What remains is the endpoint underneath the library: Yahoo's
chart JSON, fetched with `httpx` and parsed with `json`.

## One request carries its own baseline

`RANGE = "3mo"` returns ~63 daily bars rather than one. That is deliberate and it is what
makes SPEC §7.4's "moved beyond its normal range" answerable on the first day this runs: the
threshold compares the latest daily return against the standard deviation of the trailing
window, and both come out of the same response. Fetching only the last bar would mean
twenty days of accumulation before the component could say anything.

It also makes replay cheap in the way SPEC §6.3 wants — every fetch re-states the recent
past, so a missed day is repaired by the next poll rather than needing a backfill.

## Which tickers

`watchlist.tickers()`, read at fetch time. The watchlist is the reader's, hand-edited, and
its uppercase entries are exactly the tradable ones (`entities/dictionary.py`'s namespace
rule). Reading it here rather than through `SourceConfig.options` means editing the watchlist
changes tomorrow's fetch without a Terraform apply.
"""

from __future__ import annotations

import time

import httpx

from signal_core.contracts import FetchOutcome, RawDocument, SourceConfig, State
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now
from signal_core.watchlist import load as load_watchlist

# Enough history for the trailing-window standard deviation the corroboration threshold is
# measured against, with room for holidays. See the module docstring.
RANGE = "3mo"
INTERVAL = "1d"


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    tickers = sorted(load_watchlist().tickers())
    if not tickers:
        # An empty watchlist is a configuration statement, not a failure: nothing to fetch,
        # and reporting it as an error would page someone about a file they meant to empty.
        now = utc_now()
        return [], state.model_copy(update={"last_success_at": now, "consecutive_failures": 0})

    with httpx.Client(
        base_url=config.url,
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    ) as client:
        min_interval = 1.0 / config.rate_limit_per_sec if config.rate_limit_per_sec else 0.0
        documents = []
        for index, ticker in enumerate(tickers):
            if index:
                time.sleep(min_interval)
            documents.append(_fetch_ticker(client, config, ticker))

        now = utc_now()
        # Content movement is measured over the fetched payloads collectively, because the
        # honest question for this source is "did the market data advance", not "did any
        # single ticker move". A frozen upstream returns byte-identical bodies for every
        # ticker; a weekend returns the same last bar with a fresh envelope, which is why
        # the hash is over the payloads themselves rather than over the fetch time.
        digest = content_hash(b"".join(sorted(d.content_hash.encode() for d in documents)))
        moved = digest != state.last_content_hash
        new_state = state.model_copy(
            update={
                "last_success_at": now,
                "consecutive_failures": 0,
                "last_content_hash": digest,
                "last_content_change_at": (now if moved else state.last_content_change_at),
            }
        )
        return documents, new_state


def _fetch_ticker(client: httpx.Client, config: SourceConfig, ticker: str) -> RawDocument:
    now = utc_now()
    started = time.monotonic()
    path = f"/v8/finance/chart/{ticker}"
    params = {"interval": INTERVAL, "range": RANGE}
    source_url = f"{config.url}{path}?interval={INTERVAL}&range={RANGE}"
    try:
        response = client.get(path, params=params)
        response.raise_for_status()
        payload = response.content
        http_status = response.status_code
        outcome = FetchOutcome.OK
    except httpx.HTTPStatusError as exc:
        payload = str(exc).encode("utf-8")
        http_status = exc.response.status_code
        outcome = FetchOutcome.ERROR
    except httpx.HTTPError as exc:
        payload = str(exc).encode("utf-8")
        http_status = 0
        outcome = FetchOutcome.ERROR

    return RawDocument(
        ingest_id=f"{config.source_id}-{now:%Y%m%dT%H%M%S%f}-{ticker}",
        source_id=config.source_id,
        fetched_at=now,
        source_url=source_url,
        http_status=http_status,
        outcome=outcome,
        etag=None,
        last_modified=None,
        content_hash=content_hash(payload),
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=int((time.monotonic() - started) * 1000),
        byte_count=len(payload),
    )
