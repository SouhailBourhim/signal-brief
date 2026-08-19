"""Shared RSS 2.0 + Atom walker, on stdlib `xml.etree.ElementTree`. SPEC §9;
docs/runbooks/phase-2.md 2.B.

`rss_tech`, `rss_verge`, and `rss_ars` all bind `parse_feed` directly (`parse/rss.py`);
`edgar`/`edgar_formd` reuse its XML/entry primitives but build their own `ParsedItem`
from an Atom entry, since a filing's title carries a form type and CIK an ordinary
article title does not (`parse/edgar.py`).

**Parse from bytes, never str.** A feed's declared encoding is routinely a lie — EDGAR
serves `ISO-8859-1`, which is exactly why `staging.to_record` base64s the payload rather
than decoding it (SPEC §6.1). `ElementTree.fromstring(bytes)` delegates to expat, which
reads the XML declaration itself and decodes correctly; forcing a str first (guessing
UTF-8) is what would turn EDGAR's Latin-1 bytes into mojibake before parsing even starts.
If the declared encoding is *also* wrong and expat can't parse at all, the fallback is
`payload.decode("utf-8", errors="replace")`, flagged in `ParseResult.warnings` — never a
silent drop (SPEC §6.2).

**The date bug this fixes** (docs/runbooks/phase-2.md 2.B): the old
`transform._parse_published` used `datetime.fromisoformat` for everything, which cannot
read RSS 2.0's RFC 822 `pubDate` (`Tue, 18 Aug 2026 22:32:46 +0000`) — the format
TechCrunch and Ars both emit. It silently returned `None` and set `timestamp_flagged`,
indistinguishable from SPEC §6.2's honest distrust of a genuinely absent timestamp. RSS
dates go through `email.utils.parsedate_to_datetime`; Atom's RFC 3339
(`2026-08-18T16:01:01-04:00`) is what `datetime.fromisoformat` actually handles.
"""

from __future__ import annotations

import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

from signal_core.parse.models import ParsedItem, ParseResult
from signal_core.timeutil import ensure_utc

# Matches the XML declaration's encoding attribute so the fallback decode can strip it
# before reparsing as a plain (already-decoded) str — ElementTree refuses a `str` that
# still declares a non-Unicode encoding.
_ENCODING_DECL = re.compile(rb'encoding\s*=\s*["\'][^"\']*["\']')


class FeedParseError(Exception):
    """The row could not be parsed at all, even after the fallback decode."""


def local_name(tag: str) -> str:
    """Strip a `{namespace}` prefix. Feeds vary their Atom/RSS-module namespace
    prefixes; matching on the unqualified name is what makes one walker work across
    RSS 2.0's bare tags and Atom's `{http://www.w3.org/2005/Atom}`-qualified ones."""
    return tag.rsplit("}", 1)[-1]


def child_text(elem: ET.Element, name: str) -> str:
    """First direct child whose local name matches, text stripped. Empty string, never
    `None`, so callers don't need a second null check on top of the length check."""
    for child in elem:
        if local_name(child.tag) == name:
            return (child.text or "").strip()
    return ""


def atom_link_href(entry: ET.Element, rel: str = "alternate") -> str:
    """The `href` of `<link rel="alternate">` — Atom carries the URL as an attribute,
    not text, unlike everything else this module reads. Falls back to any `<link>` with
    no `rel` at all, which is legal Atom and defaults to `alternate`."""
    fallback = ""
    for child in entry:
        if local_name(child.tag) != "link":
            continue
        href = child.get("href", "")
        if child.get("rel", rel) == rel:
            return href
        fallback = fallback or href
    return fallback


def parse_rfc822(value: str) -> datetime | None:
    """RSS 2.0's `pubDate`. Malformed input is data about the source, not a reason to
    raise (SPEC §6.2) — the caller gets `None` and flags it downstream."""
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return ensure_utc(parsed)


def parse_rfc3339(value: str) -> datetime | None:
    """Atom's `published`/`updated`. `fromisoformat` is the right tool here — it is RFC
    3339 that is broken by the *RSS* case, not this one."""
    if not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def parse_xml(payload: bytes) -> tuple[ET.Element, tuple[str, ...]]:
    """Bytes -> root element, warnings. Raises `FeedParseError` only if both the
    declared encoding and the UTF-8-replace fallback fail to produce parseable XML."""
    try:
        return ET.fromstring(payload), ()
    except ET.ParseError as exc:
        # Strip the encoding declaration before reparsing: ElementTree refuses a `str`
        # that still names a non-Unicode encoding, and the string below has already
        # been through `errors="replace"`, so the declaration would be a lie twice over.
        stripped = _ENCODING_DECL.sub(b"", payload).decode("utf-8", errors="replace")
        try:
            return ET.fromstring(stripped), ("decoded_with_utf8_replace_fallback",)
        except ET.ParseError:
            raise FeedParseError(f"xml_parse_failed: {exc}") from exc


def _rss_item(item: ET.Element) -> ParsedItem:
    title = child_text(item, "title")
    link = child_text(item, "link")
    guid = child_text(item, "guid") or link
    body = child_text(item, "encoded") or child_text(item, "description")
    published_at = parse_rfc822(child_text(item, "pubDate"))

    if not title or not link:
        return ParsedItem(
            external_id=guid,
            url=link,
            title=title,
            body=body,
            published_at=published_at,
            parse_error="missing_title_or_url",
        )
    return ParsedItem(
        external_id=guid,
        url=link,
        title=title,
        body=body,
        published_at=published_at,
    )


def _atom_entry(entry: ET.Element) -> ParsedItem:
    title = child_text(entry, "title")
    url = atom_link_href(entry)
    entry_id = child_text(entry, "id") or url
    body = child_text(entry, "content") or child_text(entry, "summary")
    # `published` is when the entry first appeared; `updated` is the only timestamp
    # some Atom producers (EDGAR) emit at all, so it is a legitimate fallback rather
    # than a guess.
    published_at = parse_rfc3339(child_text(entry, "published")) or parse_rfc3339(
        child_text(entry, "updated")
    )

    if not title or not url:
        return ParsedItem(
            external_id=entry_id,
            url=url,
            title=title,
            body=body,
            published_at=published_at,
            parse_error="missing_title_or_url",
        )
    return ParsedItem(
        external_id=entry_id,
        url=url,
        title=title,
        body=body,
        published_at=published_at,
    )


def parse_feed(payload: bytes) -> ParseResult:
    """RSS 2.0 or Atom, bytes in, `ParsedItem`s out. Detects the format from the root
    element rather than trusting `SourceConfig.payload_format`, which only distinguishes
    XML from JSON/HTML, not RSS from Atom."""
    try:
        root, warnings = parse_xml(payload)
    except FeedParseError as exc:
        return ParseResult(error=str(exc))

    root_name = local_name(root.tag)
    if root_name == "rss":
        items = [_rss_item(e) for e in root.iter() if local_name(e.tag) == "item"]
    elif root_name == "feed":
        items = [_atom_entry(e) for e in root if local_name(e.tag) == "entry"]
    else:
        return ParseResult(error=f"unrecognized_feed_root: {root.tag}", warnings=warnings)

    return ParseResult(items=items, warnings=warnings)
