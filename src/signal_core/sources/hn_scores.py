"""Hacker News score snapshots. SPEC §7.4's velocity component.

## Why this is a second poller and not a flag on the first

`sources/hackernews.py` walks item ids forward from a watermark and fetches each id exactly
once. That is correct for archiving stories and it is precisely why velocity has never been
computable: **a story is fetched at the moment it is created, when its score is 1**, so the
corpus holds one point per item and there is no slope to take. `docs/runbooks/phase-2.md`
recorded this as structurally unavailable and SPEC §12 carried it into 4A.

This poller does the opposite thing on purpose. It reads `topstories.json` — the ids
currently ranked, which is a *set that changes*, not a frontier that advances — and re-fetches
them every poll. Re-reading an id already seen is the entire point here, so this module
deliberately does not consult `State.seen` or advance `State.watermark`. Folding both
behaviours into one module would leave `State` meaning two different things depending on
which caller was looking at it.

## What a snapshot costs

`topstories.json` returns ~500 ids. Fetching all of them every 15 minutes is 48,000 requests
a day against an API with no published rate limit and a request for good behaviour, to
measure a slope the brief reads once. `TOP_N` takes the front of the list instead: a story
outside the top 60 is not going to lead a brief, and if it climbs into contention it starts
being sampled on the way. The slope is measured where it is about to matter.
"""

from __future__ import annotations

import json
import time

import httpx

from signal_core.contracts import FetchOutcome, RawDocument, SourceConfig, State
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now

# How far down the ranking to sample. See the module docstring: the front of the list is
# where a brief's candidates are, and the tail is 440 requests to watch nothing happen.
TOP_N = 60


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    with httpx.Client(
        base_url=config.url,
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    ) as client:
        top = _fetch_top(client)
        if top is None:
            failed = state.model_copy(
                update={"consecutive_failures": state.consecutive_failures + 1}
            )
            return [], failed

        min_interval = 1.0 / config.rate_limit_per_sec if config.rate_limit_per_sec else 0.0
        documents: list[RawDocument] = []
        for position, item_id in enumerate(top[:TOP_N]):
            if position:
                time.sleep(min_interval)
            documents.append(_fetch_item(client, config, item_id))

        now = utc_now()
        # Content movement, in this source's terms. `last_content_hash` is over the ranked
        # id list, not over any item: scores change constantly and would report movement
        # even from a frozen API, whereas the *ranking* going static is exactly the failure
        # this signal exists to catch (SPEC §11, 1.E). A watermark would be meaningless
        # here — there is no frontier, only a set that reshuffles.
        ranking_hash = content_hash(json.dumps(top[:TOP_N]).encode("utf-8"))
        moved = ranking_hash != state.last_content_hash
        new_state = state.model_copy(
            update={
                "last_success_at": now,
                "consecutive_failures": 0,
                "last_content_hash": ranking_hash,
                "last_content_change_at": (now if moved else state.last_content_change_at),
            }
        )
        return documents, new_state


def _fetch_top(client: httpx.Client) -> list[int] | None:
    try:
        response = client.get("/topstories.json")
        response.raise_for_status()
        ids = response.json()
        if not isinstance(ids, list):
            return None
        return [int(i) for i in ids]
    except (httpx.HTTPError, ValueError, TypeError):
        return None


def _fetch_item(client: httpx.Client, config: SourceConfig, item_id: int) -> RawDocument:
    now = utc_now()
    started = time.monotonic()
    source_url = f"{config.url}/item/{item_id}.json"
    try:
        response = client.get(f"/item/{item_id}.json")
        response.raise_for_status()
        payload = response.content
        http_status = response.status_code
        outcome = FetchOutcome.EMPTY if response.text.strip() == "null" else FetchOutcome.OK
    except httpx.HTTPStatusError as exc:
        payload = str(exc).encode("utf-8")
        http_status = exc.response.status_code
        outcome = FetchOutcome.ERROR
    except httpx.HTTPError as exc:
        payload = str(exc).encode("utf-8")
        http_status = 0
        outcome = FetchOutcome.ERROR

    return RawDocument(
        # The timestamp is load-bearing in a way it is not for `hackernews`: that source
        # fetches an id once, so id alone would nearly do. Here the same id is fetched every
        # poll forever, and each fetch is a distinct observation that must not MERGE away
        # against the last one in `commit_bronze`.
        ingest_id=f"{config.source_id}-{now:%Y%m%dT%H%M%S%f}-{item_id}",
        source_id=config.source_id,
        fetched_at=now,
        source_url=source_url,
        http_status=http_status,
        outcome=outcome,
        etag=None,
        last_modified=None,
        content_hash=content_hash(payload),
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=int((time.monotonic() - started) * 1000),
        byte_count=len(payload),
    )


# A note on what is deliberately not captured. Rank-at-snapshot is a real velocity signal and
# it is observable only here — HN's item endpoint reports a story's score but not where it
# currently sits. It is dropped anyway: carrying it would mean a new column on `RawDocument`,
# whose docstring records that fetch metadata is first-class fields "rather than a loose dict"
# and whose table is the immutable record every other stage replays from. SPEC §7.4 asks for
# "HN score slope", and score is in the payload. Widening the bronze schema for a signal the
# spec did not ask for is not a trade this phase needs to make.
