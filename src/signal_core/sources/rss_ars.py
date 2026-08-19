"""Ars Technica — the third tech RSS publisher, and source #6. Phase 2 of SPEC §3.

SPEC §3 sets a constraint rather than a feature: *"adding source #6 must be a 30-minute
job"*. This is source #6, so it is the one that tests the claim instead of asserting it —
the elapsed time is recorded in `docs/runbooks/phase-2.md`. Everything it took is this
module, one `REGISTRY` line, one `SOURCES` entry, and one `var.sources` entry.

Where The Verge was chosen for overlap with TechCrunch, Ars is chosen for **depth**: it
runs long-form technical and science reporting that the other two do not, so it widens what
the brief can be about rather than only thickening the clusters.

RSS 2.0 (not Atom), and it serves `Last-Modified` but no `ETag` (measured 2026-08-19) —
so `feed.poll_feed` conditions this source with `If-Modified-Since` alone. Its dates are
RFC 822 `pubDate` (`Tue, 18 Aug 2026 22:32:46 +0000`), which is the format
`datetime.fromisoformat` cannot read at all; the parser uses
`email.utils.parsedate_to_datetime`. That distinction is a silent-`None` bug if it is ever
forgotten, so it is tested rather than remembered.

Feed window only, so `BackfillHorizon.WINDOW` — see `ops/recovery.py` for what that costs
after an outage.

This module is a thin binding of `feed.poll_feed`; see `feed.py` for the fetch logic.
"""

from __future__ import annotations

from signal_core.contracts import RawDocument, SourceConfig, State
from signal_core.sources.feed import poll_feed


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    return poll_feed(config, state)
