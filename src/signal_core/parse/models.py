"""Source-agnostic shapes a parser hands back. SPEC §9; docs/runbooks/phase-2.md 2.B.

`parse.get_parser(source_id)` returns a function from raw bronze payload bytes to a
`ParseResult`. Everything past this point — `transform.to_article`, the eventual
`silver.hn_comments` sink — works off these dataclasses and never touches a feed's XML
or JSON shape again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ParsedItem:
    """One article or filing, independent of which source it came from.

    `external_id` is the source's own identifier — an RSS `guid`, an EDGAR accession
    number, an HN item id — kept for traceability even though `transform.to_article`
    derives `article_id` from content, not from this.

    `parse_error` is set when the *entry* itself is malformed (missing a title or a
    URL) rather than the whole bronze row — a bad `<item>` in an otherwise good
    20-entry feed must not cost the other 19 (SPEC §6.2). `to_article` carries this
    straight into `silver.articles.parse_error`.
    """

    external_id: str
    url: str
    title: str
    body: str
    published_at: datetime | None
    lang: str = "en"
    extra: dict[str, Any] = field(default_factory=dict)
    parse_error: str | None = None


@dataclass(frozen=True)
class ParsedComment:
    """One Hacker News comment. Deliberately not a `ParsedItem` — see
    docs/runbooks/phase-2.md's `silver.hn_comments` finding: mapping comments into
    `articles` would inflate "articles in" roughly 10x and make the dedup ratio
    meaningless.

    `story_id` is left for 2.C to resolve by walking `parent_id` against ids already
    committed; a single comment payload only ever carries its immediate parent, never
    the root story.
    """

    item_id: str
    parent_id: str | None
    by: str | None
    text: str
    created_at: datetime | None
    dead: bool = False
    deleted: bool = False


@dataclass(frozen=True)
class ParseResult:
    """What one bronze row parses into.

    A feed document is 1 bronze row -> N items; Hacker News and `fake` are 1 row -> 0
    or 1. `error` is set only when the *row* could not be parsed at all — unrecognized
    format, XML that doesn't parse even after the encoding-lie fallback — never when a
    single entry within an otherwise-good feed is missing a field; that entry still
    comes back in `items`, carrying its own `parse_error`.
    """

    items: list[ParsedItem] = field(default_factory=list)
    comments: list[ParsedComment] = field(default_factory=list)
    error: str | None = None
    warnings: tuple[str, ...] = ()
