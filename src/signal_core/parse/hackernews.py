"""Hacker News item JSON -> `ParsedItem` or `ParsedComment`. SPEC §3, §9;
docs/runbooks/phase-2.md 2.B.

`type in ("story", "job")` becomes an article; `type == "comment"` becomes a comment
routed to `silver.hn_comments`, never `silver.articles` — see
`docs/runbooks/phase-2.md`'s velocity finding for why that split exists. One bronze row
here is already one item (unlike the feed sources' 1-row-to-N-items), so this parser
always returns at most one `items` or one `comments` entry.

Real captured shapes (2026-08-19), because HN's dense id walk means most fetched ids
are comments, not stories:

    story:   {"by":"theriddlr","id":49350858,"score":1,"time":1787079578,
              "title":"AI Is Upending One of Finance's Cushiest Jobs","type":"story",
              "url":"https://..."}
    ask:     {"by":"brianpan","id":49351430,"score":1,"text":"...","type":"story"}   # no url
    comment: {"by":"ericmay","id":49350860,"parent":49348545,"text":"...","type":"comment"}
    dead:    {"by":"grandimam","dead":true,"id":49350864,"score":1,"time":...,"type":"story"}
    deleted id: a bare `null` body — `outcome=EMPTY`, never reaches this function
    (caller skips `outcome IN ('error','empty')` rows before parsing at all).

No captured `type=="job"` item exists in the current window — jobs are rare on HN — so
that fixture (`tests/fixtures/bronze/hackernews/job.json`) is hand-built to the
documented API shape rather than pulled from real bronze; every other fixture here is.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from signal_core.parse.models import ParsedComment, ParsedItem, ParseResult

_DISCUSSION_URL = "https://news.ycombinator.com/item?id={id}"


def _epoch(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


def _story_or_job(item: dict[str, Any], item_id: str, created_at: datetime | None) -> ParseResult:
    title = (item.get("title") or "").strip()
    # Self-posts ("Ask HN", "Show HN") carry no `url`; the discussion thread is the
    # only URL that exists for them, not a missing field.
    url = item.get("url") or _DISCUSSION_URL.format(id=item_id)
    body = (item.get("text") or "").strip()

    if not title:
        return ParseResult(
            items=[
                ParsedItem(
                    external_id=item_id,
                    url=url,
                    title=title,
                    body=body,
                    published_at=created_at,
                    parse_error="missing_title",
                )
            ]
        )
    return ParseResult(
        items=[
            ParsedItem(
                external_id=item_id,
                url=url,
                title=title,
                body=body,
                published_at=created_at,
                extra={
                    "by": item.get("by"),
                    "score": item.get("score"),
                    "type": item.get("type"),
                    "dead": bool(item.get("dead", False)),
                },
            )
        ]
    )


def _comment(item: dict[str, Any], item_id: str, created_at: datetime | None) -> ParseResult:
    parent = item.get("parent")
    return ParseResult(
        comments=[
            ParsedComment(
                item_id=item_id,
                parent_id=str(parent) if parent is not None else None,
                by=item.get("by"),
                text=(item.get("text") or "").strip(),
                created_at=created_at,
                dead=bool(item.get("dead", False)),
                deleted=bool(item.get("deleted", False)),
            )
        ]
    )


def parse(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8", errors="replace")
    try:
        item = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParseResult(error=f"payload_not_json: {exc}")

    if item is None:
        # A bare `null` body — HN's shape for a deleted/not-yet-visible id. Bronze
        # already tags this `outcome=EMPTY` (SPEC §6.2); nothing to parse.
        return ParseResult()

    item_id = str(item.get("id", ""))
    created_at = _epoch(item.get("time"))
    item_type = item.get("type")

    if item_type in ("story", "job"):
        return _story_or_job(item, item_id, created_at)
    if item_type == "comment":
        return _comment(item, item_id, created_at)

    # `poll`/`pollopt`, or a type HN adds later — outside SPEC §3's scope. Noted rather
    # than silently absorbed, so a real one showing up in production is visible.
    return ParseResult(warnings=(f"unhandled_hn_type: {item_type!r}",))
