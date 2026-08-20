"""Hacker News poller. SPEC §3, §6.1.

Item ids are sequential and dense, so catching up after downtime is a range walk from
the last watermark to whatever `maxitem` reports now — no feed window to lose, which is
why this source's backfill horizon is COMPLETE (SPEC §3). Score is time-dependent: each
poll snapshots an item as its own `RawDocument` rather than ever overwriting a previous
fetch (SPEC §3, §6.2).

Unlike `feed.py`'s single-fetch sources, one poll here means many small requests, so this
module paces them itself against `SourceConfig.rate_limit_per_sec` rather than relying on
the poll cadence alone.
"""

from __future__ import annotations

import time

import httpx

from signal_core.contracts import FetchOutcome, RawDocument, SourceConfig, State
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now

# Bounds one invocation's work. A large backlog spreads across subsequent polls rather
# than risking a timeout — safe only because the horizon is COMPLETE: nothing is lost by
# being late, unlike the WINDOW/DAY sources in feed.py.
MAX_ITEMS_PER_POLL = 200


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    with httpx.Client(
        base_url=config.url,
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    ) as client:
        maxitem = _fetch_maxitem(client)
        if maxitem is None:
            failed = state.model_copy(
                update={"consecutive_failures": state.consecutive_failures + 1}
            )
            return [], failed

        # First-ever poll starts at the current frontier rather than backfilling HN's
        # entire history from item 1.
        watermark = state.watermark if isinstance(state.watermark, int) else None
        start = watermark + 1 if watermark is not None else maxitem
        end = min(maxitem, start + MAX_ITEMS_PER_POLL - 1)

        min_interval = 1.0 / config.rate_limit_per_sec if config.rate_limit_per_sec else 0.0
        documents: list[RawDocument] = []
        for item_id in range(start, end + 1):
            if item_id > start:
                time.sleep(min_interval)
            documents.append(_fetch_item(client, config, item_id))
            watermark = item_id

        now = utc_now()
        # SPEC §11's content-movement signal, in this source's own terms. There is no
        # body to hash here — a poll fetches many items, each already unique by id — so
        # the honest question is whether HN produced anything new since last time, which
        # is exactly "did the watermark advance". A frozen `maxitem` means `start >
        # maxitem`, the range is empty, and this correctly records no movement while
        # `last_success_at` still advances (the fetch did work).
        content_moved = bool(documents)
        new_state = state.model_copy(
            update={
                "watermark": watermark,
                "last_success_at": now,
                "consecutive_failures": 0,
                "last_content_change_at": (now if content_moved else state.last_content_change_at),
            }
        )
        return documents, new_state


def _fetch_maxitem(client: httpx.Client) -> int | None:
    try:
        response = client.get("/maxitem.json")
        response.raise_for_status()
        return int(response.text)
    except (httpx.HTTPError, ValueError):
        # Can't even find the frontier this invocation; try again next minute.
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
        # A bare `null` body means the id is deleted or not yet visible — a real,
        # expected outcome, not an error (SPEC §6.2: never silently drop a record).
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
