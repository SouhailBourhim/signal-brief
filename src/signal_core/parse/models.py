"""Source-agnostic shapes a parser hands back. SPEC §9; docs/runbooks/phase-2.md 2.B.

`parse.get_parser(source_id)` returns a function from raw bronze payload bytes to a
`ParseResult`. Everything past this point — `transform.to_article`, the eventual
`silver.hn_comments` sink — works off these dataclasses and never touches a feed's XML
or JSON shape again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
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
class ParsedScoreSnapshot:
    """One observation of a Hacker News story's score at a point in time. SPEC §7.4.

    Deliberately not a `ParsedItem`, for the same reason `ParsedComment` is not: routing
    these into `silver.articles` would multiply "articles in" by the number of times each
    story is sampled and make the dedup ratio meaningless. It is also not the same *kind*
    of record — a `ParsedItem` is a document, this is a measurement of one.

    `observed_at` is the fetch time, taken from the bronze row rather than the payload. The
    payload's `time` is when the story was *submitted* and never moves; what makes a slope
    is when we looked. That is why this dataclass leaves it unset and `normalize` fills it
    from `fetched_at` — the parser cannot see it.
    """

    item_id: str
    score: int
    descendants: int | None = None
    title: str = ""


@dataclass(frozen=True)
class ParsedMarketObservation:
    """One daily OHLCV bar for one ticker. SPEC §7.4's market-corroboration input.

    `trade_date` is a date, not a timestamp: Stooq serves daily bars and the close is the
    only price the ranker asks about. Volume is carried because a move on no volume is a
    different claim than a move on heavy volume, and the threshold may later want it.
    """

    ticker: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


@dataclass(frozen=True)
class ParsedMacroObservation:
    """One value of one macro series, as of one vintage. SPEC §8's two time axes.

    `period` is **valid time** — the month or day the number describes. `vintage_date` is
    **known time** — the day that value became the published figure. The pair is what makes
    "what was knowable on 2026-03-14" a query.

    `superseded_at` is ALFRED's `realtime_end`, carried rather than collapsed to a boolean:
    a `None` here means "still the current value", and the date it stopped being current is a
    fact the bitemporal load would otherwise have to re-derive from the next vintage's start.

    `revision_delta` is deliberately **not** a field. It is derived in
    `spark/jobs/macro.py` with a window over each series and period, because it is a
    relationship between two vintages and a parser only ever sees one at a time.
    """

    series_id: str
    period: date
    value: float | None
    vintage_date: date
    superseded_at: date | None = None


@dataclass(frozen=True)
class ParseResult:
    """What one bronze row parses into.

    A feed document is 1 bronze row -> N items; Hacker News and `fake` are 1 row -> 0
    or 1. `error` is set only when the *row* could not be parsed at all — unrecognized
    format, XML that doesn't parse even after the encoding-lie fallback — never when a
    single entry within an otherwise-good feed is missing a field; that entry still
    comes back in `items`, carrying its own `parse_error`.

    The five collections are parallel sinks, not alternatives: a parser fills exactly the
    one its source produces. Adding a shape here rather than overloading `items` is what
    keeps `dedup_ratio` an honest number (docs/runbooks/phase-2.md's comments finding).
    """

    items: list[ParsedItem] = field(default_factory=list)
    comments: list[ParsedComment] = field(default_factory=list)
    score_snapshots: list[ParsedScoreSnapshot] = field(default_factory=list)
    market_observations: list[ParsedMarketObservation] = field(default_factory=list)
    macro_observations: list[ParsedMacroObservation] = field(default_factory=list)
    error: str | None = None
    warnings: tuple[str, ...] = ()
