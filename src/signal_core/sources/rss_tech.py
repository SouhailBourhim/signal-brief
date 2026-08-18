"""One tech RSS feed (TechCrunch). Phase 1 of SPEC §3's source list.

Feed window only (~1-3h) — SPEC §3: `pubDate` is often wrong or absent, and feeds go
stale while still returning 200, which is why §11's staleness check looks at content
movement rather than trusting a 200 status. This module is a thin binding of
`feed.poll_feed` to this source's config; see `feed.py` for the actual fetch logic.
"""

from __future__ import annotations

from signal_core.contracts import RawDocument, SourceConfig, State
from signal_core.sources.feed import poll_feed


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    return poll_feed(config, state)
