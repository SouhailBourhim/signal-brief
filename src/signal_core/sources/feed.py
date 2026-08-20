"""Shared conditional-GET fetch for single-feed sources. SPEC §6.1, §6.2.

`rss_tech` and `edgar` are the same shape: one URL, conditioned on the last known
`ETag`/`Last-Modified`, handed back whole as a single `RawDocument` — no parsing, per
§6.1 ("the adapter's only job is to fetch and hand back bytes plus metadata"). What
differs between the two sources lives entirely in `SourceConfig`, which is the point of
§6's contract, so both modules call this one function.

Failures are data, not exceptions: an unreachable feed or a non-2xx/304 response becomes
a `RawDocument` with `outcome=ERROR` rather than raising, so a poller never crashes a
Lambda invocation over a routine feed hiccup. `ops.health.assess_source` (SPEC §11) is
what turns a run of these into an alert — via staleness, not a Lambda error count.
"""

from __future__ import annotations

import time

import httpx

from signal_core.contracts import FetchOutcome, RawDocument, SourceConfig, State
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now


def _ingest_id(source_id: str, suffix: str = "") -> str:
    return f"{source_id}-{utc_now():%Y%m%dT%H%M%S%f}{suffix}"


def poll_feed(
    config: SourceConfig,
    state: State,
    *,
    client: httpx.Client | None = None,
) -> tuple[list[RawDocument], State]:
    headers = {"User-Agent": config.user_agent}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    owns_client = client is None
    client = client or httpx.Client(timeout=config.timeout_seconds)
    started = time.monotonic()
    try:
        response = client.get(config.url, headers=headers)
    except httpx.HTTPError as exc:
        return _error_result(config, state, started, http_status=0, detail=str(exc))
    finally:
        if owns_client:
            client.close()

    latency_ms = int((time.monotonic() - started) * 1000)
    now = utc_now()

    if response.status_code == 304:
        # Healthy and unchanged — most polls should land here (SPEC §6.2). Deliberately
        # does NOT *advance* `last_content_change_at`: a 304 is the server stating outright
        # that the content has not moved, which is the exact case §11's dead-feed check
        # exists to accumulate.
        #
        # It does **seed** it when unset, which is not the same thing. A source that 304s
        # from its very first poll — `rss_ars` does, it serves `Last-Modified` and changes
        # roughly once every seven hours — would otherwise hold `None` until it happened to
        # publish, and `assess_source` reads `None` as "no signal, skip the check". The
        # lowest-volume source, the one most likely to die unnoticed, would have been the
        # one source exempt from dead-feed detection. Found in production, 2026-08-20.
        #
        # Seeding to `now` starts the clock when observation starts, which is the most that
        # can be honestly claimed: nothing is knowable about content movement before the
        # first poll. If the feed published moments before, the clock is late by that much —
        # bounded, and in the conservative direction (late to flag, never early).
        update: dict[str, object] = {"last_success_at": now, "consecutive_failures": 0}
        if state.last_content_change_at is None:
            update["last_content_change_at"] = now
        new_state = state.model_copy(update=update)
        return [], new_state

    if not response.is_success:
        return _error_result(
            config,
            state,
            started,
            http_status=response.status_code,
            detail=response.text[:500],
        )

    payload = response.content
    etag = response.headers.get("etag", state.etag)
    last_modified = response.headers.get("last-modified", state.last_modified)
    payload_hash = content_hash(payload)
    doc = RawDocument(
        ingest_id=_ingest_id(config.source_id),
        source_id=config.source_id,
        fetched_at=now,
        source_url=config.url,
        http_status=response.status_code,
        outcome=FetchOutcome.OK if payload else FetchOutcome.EMPTY,
        etag=etag,
        last_modified=last_modified,
        content_hash=payload_hash,
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=latency_ms,
        byte_count=len(payload),
    )
    # A 200 whose body is byte-identical to the last one is the dead-feed case SPEC §11
    # names: the fetch succeeded, and nothing moved. Advancing `last_content_change_at`
    # here would erase the only evidence of it. Both SEC sources make this concrete —
    # `browse-edgar` is a CGI script serving no validators, so every poll is a full body
    # and a status code can never reveal a frozen feed (docs/runbooks/phase-2.md 2.A).
    #
    # First-ever poll (`last_content_hash is None`) counts as a change: there is no prior
    # content for it to be identical to, and seeding it as unchanged would report a
    # brand-new source as dead until its second distinct body arrived.
    content_moved = state.last_content_hash != payload_hash
    new_state = state.model_copy(
        update={
            "etag": etag,
            "last_modified": last_modified,
            "last_success_at": now,
            "consecutive_failures": 0,
            "last_content_hash": payload_hash,
            "last_content_change_at": (now if content_moved else state.last_content_change_at),
        }
    )
    return [doc], new_state


def _error_result(
    config: SourceConfig,
    state: State,
    started: float,
    *,
    http_status: int,
    detail: str,
) -> tuple[list[RawDocument], State]:
    """A quarantined record, per SPEC §6.2: never silently dropped, never raised either.

    The last good `etag`/`last_modified` is kept so the next poll still conditions
    correctly — a failed fetch must not make the next one re-download unchanged content.
    """
    latency_ms = int((time.monotonic() - started) * 1000)
    now = utc_now()
    payload = detail.encode("utf-8")
    doc = RawDocument(
        ingest_id=_ingest_id(config.source_id, "-error"),
        source_id=config.source_id,
        fetched_at=now,
        source_url=config.url,
        http_status=http_status,
        outcome=FetchOutcome.ERROR,
        etag=state.etag,
        last_modified=state.last_modified,
        content_hash=content_hash(payload),
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=latency_ms,
        byte_count=len(payload),
    )
    new_state = state.model_copy(update={"consecutive_failures": state.consecutive_failures + 1})
    return [doc], new_state
