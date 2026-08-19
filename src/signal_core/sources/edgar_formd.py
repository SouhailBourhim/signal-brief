"""SEC Form D — startup fundraises, often ahead of the press. Phase 2 of SPEC §3.

The same `browse-edgar` current-filings endpoint as `edgar`, narrowed to `type=D`. SPEC §3
calls Form D "underused; a genuine scoop source", and it is the one source here that
regularly reports a raise before anyone writes about it.

**Backfill horizon is DAY, not COMPLETE.** SPEC §3 lists Form D as complete, which is true
of the daily full-index files and not of the current-filings feed this polls. Declaring
COMPLETE here would make `ops.recovery.plan_catch_up` promise a recovery it cannot perform
and suppress the `gap_reason` that SPEC §6.3 exists to surface. The index fallback is
Phase 4+; until it is wired up, DAY is what is true of the endpoint we actually hit.

Two things measured against the live endpoint on 2026-08-19, both of which matter
downstream:

  * **It serves `encoding="ISO-8859-1"`.** This is exactly why `staging.to_record` base64s
    the payload rather than decoding it (SPEC §6.1): the bytes are the record, and
    interpreting them is Spark's job, against stored bytes, where a mistake is fixable.
  * **It returns neither `ETag` nor `Last-Modified`.** Conditional GET is therefore a
    no-op for both SEC sources: every poll transfers a full body and never 304s, so
    content movement is detectable only via `content_hash`, never via a status code.

This module is a thin binding of `feed.poll_feed`; see `feed.py` for the fetch logic.
"""

from __future__ import annotations

from signal_core.contracts import RawDocument, SourceConfig, State
from signal_core.sources.feed import poll_feed


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    return poll_feed(config, state)
