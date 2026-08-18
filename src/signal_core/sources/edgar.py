"""SEC EDGAR current filings (8-K, S-1, Form D as filed). Phase 1 of SPEC §3.

Backfill horizon is DAY, not COMPLETE — the current-filings feed only reflects roughly a
day of history; recovering further would need the daily index, which is out of scope for
Phase 1 (SPEC §3). A descriptive `User-Agent` with a contact email is not optional here:
SEC blocks fair-access violators, and `config.user_agent` (from `Settings.user_agent`)
carries it. This module is a thin binding of `feed.poll_feed`; see `feed.py` for the
actual fetch logic.
"""

from __future__ import annotations

from signal_core.contracts import RawDocument, SourceConfig, State
from signal_core.sources.feed import poll_feed


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    return poll_feed(config, state)
