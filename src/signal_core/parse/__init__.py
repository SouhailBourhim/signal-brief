"""Parser registry. SPEC §9; docs/runbooks/phase-2.md 2.B.

Mirrors `sources.REGISTRY`: `REGISTRY` maps source_id to a parser — bronze payload bytes
in, `ParseResult` out — so adding source #7 on the silver side is one module plus one
entry here, the same claim §3 makes about the ingestion side.
"""

from __future__ import annotations

from collections.abc import Callable

from signal_core.parse.edgar import parse as edgar_parse
from signal_core.parse.fake import parse as fake_parse
from signal_core.parse.hackernews import parse as hackernews_parse
from signal_core.parse.models import ParsedComment, ParsedItem, ParseResult
from signal_core.parse.rss import parse_rss_ars, parse_rss_tech, parse_rss_verge

Parser = Callable[[bytes], ParseResult]

REGISTRY: dict[str, Parser] = {
    "fake": fake_parse,
    "hackernews": hackernews_parse,
    "edgar": edgar_parse,
    # Same entry shape as `edgar` — `type=D` only narrows the feed, see parse/edgar.py.
    "edgar_formd": edgar_parse,
    "rss_tech": parse_rss_tech,
    "rss_verge": parse_rss_verge,
    "rss_ars": parse_rss_ars,
}

__all__ = ["REGISTRY", "ParseResult", "ParsedComment", "ParsedItem", "get_parser"]


def get_parser(source_id: str) -> Parser:
    try:
        return REGISTRY[source_id]
    except KeyError:
        raise KeyError(f"unknown source {source_id!r}; registered: {sorted(REGISTRY)}") from None
