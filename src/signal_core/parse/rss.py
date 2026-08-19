"""Thin bindings of `feedparse.parse_feed` for the three text-only RSS/Atom sources.
SPEC §9; docs/runbooks/phase-2.md 2.B.

Exactly the `sources/rss_tech.py` pattern on the ingestion side: no source-specific
parsing logic, because none is needed — `feedparse.py` already detects RSS 2.0 vs. Atom
from the root element. Three named functions rather than one shared alias, so a future
publisher-specific quirk (a malformed feed that needs a workaround) is a one-line change
here rather than a fork of the shared walker.
"""

from __future__ import annotations

from signal_core.parse.feedparse import parse_feed
from signal_core.parse.models import ParseResult


def parse_rss_tech(payload: bytes) -> ParseResult:
    return parse_feed(payload)


def parse_rss_verge(payload: bytes) -> ParseResult:
    return parse_feed(payload)


def parse_rss_ars(payload: bytes) -> ParseResult:
    return parse_feed(payload)
