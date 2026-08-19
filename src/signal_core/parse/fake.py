"""The Phase 0 fixture shape -> `ParsedItem`. SPEC §9; docs/runbooks/phase-2.md 2.B.

`fake` becomes just another registry entry rather than a special case `transform.py`
hardcodes, so the skeleton (`make skeleton` / `make skeleton-nospark`) exercises the
same `get_parser` -> `to_article` path real sources do, and CI catches a break in that
path even though it never touches the network.

`sources/fake.py` emits one JSON object per document —
`{id, story_key, title, body, publisher, url, published_at}` — with `published_at`
either absent (`None`) or a real `datetime.isoformat()` string, never an RFC 822 or
RFC 3339-with-lies value, so plain `datetime.fromisoformat` is correct here. That is
what makes this module different from `feedparse.py`: there is exactly one date format
to trust, because the fixture data controls it.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from signal_core.parse.models import ParsedItem, ParseResult
from signal_core.timeutil import ensure_utc


def _parse_published(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value)))
    except ValueError:
        return None


def parse(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8", errors="replace")
    try:
        article = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParseResult(error=f"payload_not_json: {exc}")

    external_id = str(article.get("id", ""))
    title = (article.get("title") or "").strip()
    url = article.get("url") or ""
    body = (article.get("body") or "").strip()

    if not title or not url:
        return ParseResult(
            items=[
                ParsedItem(
                    external_id=external_id,
                    url=url,
                    title=title,
                    body=body,
                    published_at=None,
                    parse_error="missing_title_or_url",
                )
            ]
        )

    return ParseResult(
        items=[
            ParsedItem(
                external_id=external_id,
                url=url,
                title=title,
                body=body,
                published_at=_parse_published(article.get("published_at")),
                extra={
                    "story_key": article.get("story_key"),
                    "publisher": article.get("publisher"),
                },
            )
        ]
    )
