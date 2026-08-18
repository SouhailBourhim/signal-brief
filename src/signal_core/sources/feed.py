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
        # Healthy and unchanged — most polls should land here (SPEC §6.2).
        new_state = state.model_copy(update={"last_success_at": now, "consecutive_failures": 0})
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
    doc = RawDocument(
        ingest_id=_ingest_id(config.source_id),
        source_id=config.source_id,
        fetched_at=now,
        source_url=config.url,
        http_status=response.status_code,
        outcome=FetchOutcome.OK if payload else FetchOutcome.EMPTY,
        etag=etag,
        last_modified=last_modified,
        content_hash=content_hash(payload),
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=latency_ms,
        byte_count=len(payload),
    )
    new_state = state.model_copy(
        update={
            "etag": etag,
            "last_modified": last_modified,
            "last_success_at": now,
            "consecutive_failures": 0,
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
