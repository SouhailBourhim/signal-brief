"""Yahoo chart JSON -> `ParsedMarketObservation`. SPEC §7.4; ADR-0010.

One bronze row is one ticker's recent history, so this is a 1-row-to-N-observations parser
like the feed sources, not the 1-to-1 shape Hacker News has.

The response nests the data in two parallel structures — a `timestamp` array and an
`indicators.quote[0]` object holding one array per field — which are positionally aligned:

    {"chart": {"result": [{
        "meta": {"symbol": "AAPL", ...},
        "timestamp": [1786060800, 1786147200, ...],
        "indicators": {"quote": [{"open": [...], "high": [...], "low": [...],
                                  "close": [...], "volume": [...]}]}
    }], "error": null}}

Any of those per-day values can be `null` — a halted session, a bar the exchange never
published. A row with no close is dropped rather than zero-filled: a zero close is a
catastrophic price move to anything computing returns, and SPEC §6.2's "never silently
dropped" is satisfied by the bronze row still holding the original bytes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from signal_core.parse.models import ParsedMarketObservation, ParseResult


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # NaN compares unequal to itself, and JSON can carry one through a permissive decoder.
    return None if out != out else out


def parse(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8", errors="replace")
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParseResult(error=f"payload_not_json: {exc}")

    chart = (document or {}).get("chart") or {}
    if chart.get("error"):
        # Yahoo reports an unknown ticker in-band with HTTP 200 in some cases. That is a
        # row-level failure — the bytes are exactly what the source sent — so it lands in
        # `parse_rejects` rather than pretending the ticker had no trading days.
        return ParseResult(error=f"chart_error: {chart['error']}")

    results = chart.get("result") or []
    if not results:
        return ParseResult(error="chart_error: no result block")

    result = results[0]
    ticker = ((result.get("meta") or {}).get("symbol") or "").upper()
    if not ticker:
        return ParseResult(error="chart_error: no symbol in meta")

    timestamps = result.get("timestamp") or []
    quotes = (result.get("indicators") or {}).get("quote") or [{}]
    quote = quotes[0] if quotes else {}

    observations: list[ParsedMarketObservation] = []
    dropped = 0
    for index, epoch in enumerate(timestamps):
        close = _as_float(_at(quote.get("close"), index))
        if close is None:
            dropped += 1
            continue
        try:
            trade_date = datetime.fromtimestamp(int(epoch), tz=UTC).date()
        except (TypeError, ValueError, OSError):
            dropped += 1
            continue
        # open/high/low fall back to the close rather than dropping the bar: the close is
        # the only field the ranker reads, and a bar with a real close but a null open is
        # still a usable observation.
        observations.append(
            ParsedMarketObservation(
                ticker=ticker,
                trade_date=trade_date,
                open=_as_float(_at(quote.get("open"), index)) or close,
                high=_as_float(_at(quote.get("high"), index)) or close,
                low=_as_float(_at(quote.get("low"), index)) or close,
                close=close,
                volume=_as_float(_at(quote.get("volume"), index)),
            )
        )

    warnings = (f"dropped_bars_without_close: {dropped}",) if dropped else ()
    return ParseResult(market_observations=observations, warnings=warnings)


def _at(values: Any, index: int) -> Any:
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]
