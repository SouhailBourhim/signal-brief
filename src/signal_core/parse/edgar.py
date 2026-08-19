"""SEC EDGAR current-filings Atom entries -> `ParsedItem`. SPEC §9;
docs/runbooks/phase-2.md 2.B.

One parser for both `edgar` and `edgar_formd` (`parse/__init__.py` registers it twice):
the `type=D` query parameter only narrows which filings the feed returns, it does not
change the entry shape. A real entry, captured 2026-08-19:

    <entry>
    <title>D - Klondike Holdings LLC (0002144150) (Filer)</title>
    <link rel="alternate" type="text/html"
          href="https://www.sec.gov/Archives/.../0002144150-26-000002-index.htm"/>
    <summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; 2026-08-18 &lt;b&gt;AccNo:&lt;/b&gt;
        0002144150-26-000002 &lt;b&gt;Size:&lt;/b&gt; 5 KB</summary>
    <updated>2026-08-18T17:28:48-04:00</updated>
    <category scheme="https://www.sec.gov/" label="form type" term="D"/>
    <id>urn:tag:sec.gov,2008:accession-number=0002144150-26-000002</id>
    </entry>

Form type comes off `<category term=...>` — clean, structured, and exactly what SPEC §3
calls the "interesting difficulty" of this source. The title carries the same form type
as its first token, but re-deriving it from free text when a dedicated attribute exists
would be inviting the parse to break the day SEC reformats the title and not the
category. CIK and filer name are only in the title, so those are extracted from there.

EDGAR's Atom has no `<published>`, only `<updated>` — `feedparse.parse_rfc3339` already
falls back to it, so no override is needed here.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from signal_core.parse.feedparse import (
    FeedParseError,
    atom_link_href,
    child_text,
    local_name,
    parse_rfc3339,
    parse_xml,
)
from signal_core.parse.models import ParsedItem, ParseResult

# The CIK is the only all-digit parenthesized group in the title; "(Filer)"/"(Reporting)"
# are the other candidates and are never numeric.
_CIK_RE = re.compile(r"\((\d+)\)")


def _parse_title(title: str) -> tuple[str, str]:
    """`('Klondike Holdings LLC', '0002144150')` from
    `'D - Klondike Holdings LLC (0002144150) (Filer)'`."""
    _, _, rest = title.partition(" - ")
    filer_name, _, _ = rest.partition(" (")
    match = _CIK_RE.search(title)
    return filer_name.strip(), match.group(1) if match else ""


def _entry(entry: ET.Element) -> ParsedItem:
    title = child_text(entry, "title")
    url = atom_link_href(entry)
    entry_id = child_text(entry, "id") or url
    summary = child_text(entry, "summary")
    published_at = parse_rfc3339(child_text(entry, "published")) or parse_rfc3339(
        child_text(entry, "updated")
    )
    form_type = ""
    for child in entry:
        if local_name(child.tag) == "category":
            form_type = child.get("term", "")
            break

    if not title or not url:
        return ParsedItem(
            external_id=entry_id,
            url=url,
            title=title,
            body=summary,
            published_at=published_at,
            parse_error="missing_title_or_url",
        )

    filer_name, cik = _parse_title(title)
    return ParsedItem(
        external_id=entry_id,
        url=url,
        title=title,
        body=summary,
        published_at=published_at,
        extra={"form_type": form_type, "cik": cik, "filer_name": filer_name},
    )


def parse(payload: bytes) -> ParseResult:
    try:
        root, warnings = parse_xml(payload)
    except FeedParseError as exc:
        return ParseResult(error=str(exc))

    if local_name(root.tag) != "feed":
        return ParseResult(error=f"unrecognized_feed_root: {root.tag}", warnings=warnings)

    items = [_entry(e) for e in root if local_name(e.tag) == "entry"]
    return ParseResult(items=items, warnings=warnings)
