"""The Verge — the second tech RSS publisher. Phase 2 of SPEC §3.

Chosen for **overlap**, not coverage. The Verge and TechCrunch report the same acquisitions
and launches within minutes of each other, which is the case SPEC §7.1 exists for: a story
arriving as N articles from N publishers is what makes `distinct_publisher_count` mean
something and what makes Phase 3's dedup ratio a measurement rather than a definition. A
second publisher covering disjoint ground would add articles and prove nothing.

Atom, UTF-8, and it does serve an `ETag` (measured 2026-08-19), so the conditional-GET path
in `feed.py` actually engages here — unlike the two SEC sources, where it is inert.

Feed window only, so `BackfillHorizon.WINDOW`: items published and rotated out during an
outage are gone, and `ops.recovery` says so rather than implying a quiet day (SPEC §6.3).

This module is a thin binding of `feed.poll_feed`; see `feed.py` for the fetch logic.
"""

from __future__ import annotations

from signal_core.contracts import RawDocument, SourceConfig, State
from signal_core.sources.feed import poll_feed


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    return poll_feed(config, state)
