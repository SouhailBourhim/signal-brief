"""Hacker News item JSON -> `ParsedScoreSnapshot`. SPEC §7.4's velocity component.

The payload shape is identical to `parse/hackernews.py`'s story case — same endpoint, same
JSON — so this module is deliberately thin. What differs is which field is the *record*:
`hackernews` reads the title and url and treats `score` as incidental `extra`; here `score`
is the observation and the rest is context.

The two parsers stay separate rather than one parser returning both shapes, because the two
sources sample different id sets on different cadences, and a single parser returning an
article *and* a snapshot for every fetch would put every top-story into `silver.articles`
a second time on every poll.

`observed_at` is not set here. It is the bronze row's `fetched_at`, which a parser working
from payload bytes alone cannot see — the payload's own `time` is the submission time and
never moves, so using it would give every snapshot of a story the same timestamp and a
slope of zero over an infinite interval. `normalize` supplies it.
"""

from __future__ import annotations

import json
from typing import Any

from signal_core.parse.models import ParsedScoreSnapshot, ParseResult


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8", errors="replace")
    try:
        item = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParseResult(error=f"payload_not_json: {exc}")

    if item is None:
        # Deleted or not yet visible. Bronze already tags this EMPTY (SPEC §6.2).
        return ParseResult()

    item_type = item.get("type")
    if item_type not in ("story", "job"):
        # A comment can reach the top-stories list only if HN changes what that list
        # means. Noted rather than absorbed, matching `parse/hackernews.py`.
        return ParseResult(warnings=(f"unhandled_hn_type: {item_type!r}",))

    score = _as_int(item.get("score"))
    if score is None:
        # A story with no score is a shape this parser does not understand. It is not an
        # error for the *row* — the bytes are fine — so it warns rather than failing,
        # and contributes no snapshot rather than a zero that would read as a real
        # observation and drag a slope down.
        return ParseResult(warnings=(f"missing_score: {item.get('id')!r}",))

    return ParseResult(
        score_snapshots=[
            ParsedScoreSnapshot(
                item_id=str(item.get("id", "")),
                score=score,
                descendants=_as_int(item.get("descendants")),
                title=(item.get("title") or "").strip(),
            )
        ]
    )
