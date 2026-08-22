"""ALFRED observations JSON -> `ParsedMacroObservation`. SPEC §8.

One bronze row is one series' full vintage history inside the bounded window, so this is a
1-row-to-N-observations parser like the feeds, not the 1-to-1 shape Hacker News has.

The response is flat, which is a relief after Yahoo's parallel arrays:

    {"realtime_start": "2015-01-01", "realtime_end": "9999-12-31",
     "observation_start": "1776-07-04", "count": 1234, "offset": 0, "limit": 100000,
     "observations": [
       {"realtime_start": "2026-07-02", "realtime_end": "2026-08-06",
        "date": "2026-06-01", "value": "159842"},
       {"realtime_start": "2026-08-07", "realtime_end": "9999-12-31",
        "date": "2026-06-01", "value": "159796"}, ...]}

Those two rows are the whole point of §8: the same month, published twice, revised down by
46. A pipeline that overwrote the first would destroy the fact the brief exists to state.

## Three things about the payload that are easy to get wrong

- **`value` is a string, and `"."` means missing.** FRED's own sentinel for "no observation
  for this period". It becomes `None`, not `0.0` — a zero unemployment rate is a very
  different claim from an unpublished one, and `revision_delta` computed against a
  zero-filled gap would report a fictional revision the size of the whole series.
- **`realtime_end` of `9999-12-31` means "still current"**, not a date in the year 9999. It
  becomes `superseded_at = None`, because a sentinel that survives into the table is a
  sentinel someone eventually does arithmetic on.
- **`count` can exceed what came back.** FRED caps a response at `limit` (100,000 default),
  and a truncated series looks like one that simply stops. It is reported as a warning rather
  than silently accepted; `sources/macro.py`'s bounded window exists partly to stay well
  under it.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from urllib.parse import parse_qs, urlparse

from signal_core.parse.models import ParsedMacroObservation, ParseResult

# ALFRED's "still the current vintage" sentinel.
STILL_CURRENT = "9999-12-31"

# FRED's own missing-value marker.
MISSING = "."


def series_id_from_url(url: str | None) -> str:
    """The `series_id` query parameter from a recorded ALFRED request URL.

    Bronze stores the URL the poller actually called, so the series id is recoverable from
    the immutable record rather than needing to have been injected into the payload — which
    would have meant mutating source bytes before storing them, exactly what SPEC §6.1 says
    a poller must not do.
    """
    if not url:
        return ""
    query = urlparse(url).query
    values = parse_qs(query).get("series_id") or []
    return values[0] if values else ""


def _as_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _as_value(raw: str | None) -> float | None:
    if raw is None or raw.strip() in ("", MISSING):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8", errors="replace")
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParseResult(error=f"payload_not_json: {exc}")

    if not isinstance(document, dict):
        return ParseResult(error="payload_not_an_object")

    if "error_message" in document:
        # FRED reports a bad key or an unknown series id in-band. A row-level failure — the
        # bytes are exactly what the source sent — so it belongs in `parse_rejects` rather
        # than being read as a series with no observations.
        return ParseResult(error=f"fred_error: {document['error_message']}")

    observations = document.get("observations")
    if not isinstance(observations, list):
        return ParseResult(error="no_observations_array")

    warnings: list[str] = []
    count = document.get("count")
    if isinstance(count, int) and count > len(observations):
        warnings.append(
            f"truncated: FRED reports {count} observations, {len(observations)} returned "
            f"(limit {document.get('limit')}, offset {document.get('offset')})"
        )

    # FRED does not echo the series id anywhere in the body — it is a property of the
    # request. `spark/jobs/macro.py` recovers it from the bronze row's `source_url` with
    # `series_id_from_url` and stamps it on; leaving it empty here rather than guessing keeps
    # this a pure function of the payload, which is what makes the parser testable against a
    # committed fixture.
    series_id = ""

    parsed: list[ParsedMacroObservation] = []
    skipped = 0
    for observation in observations:
        if not isinstance(observation, dict):
            skipped += 1
            continue
        period = _as_date(observation.get("date"))
        vintage = _as_date(observation.get("realtime_start"))
        if period is None or vintage is None:
            # Both axes are the record. An observation missing either is not a partial fact,
            # it is an unplaceable one, so it is counted and left out rather than defaulted
            # onto today's date where it would look like a fresh vintage.
            skipped += 1
            continue
        raw_end = observation.get("realtime_end")
        parsed.append(
            ParsedMacroObservation(
                series_id=series_id,
                period=period,
                value=_as_value(observation.get("value")),
                vintage_date=vintage,
                superseded_at=None if raw_end == STILL_CURRENT else _as_date(raw_end),
            )
        )

    if skipped:
        warnings.append(f"{skipped} observations skipped: unparseable date or realtime_start")

    return ParseResult(macro_observations=parsed, warnings=tuple(warnings))
