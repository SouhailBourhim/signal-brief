"""The ingestion contract. SPEC §6.1.

Every source implements `poll(config, state) -> (list[RawDocument], State)`. That
signature is the whole extensibility story: adding source #6 means writing one module
that satisfies this protocol and adding one Terraform map entry.

A poller fetches bytes and reports how the fetch went. It does not parse, normalize,
filter, or interpret — everything interpretive happens later in Spark, where it can be
re-run against stored bytes without touching the network.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PayloadFormat(StrEnum):
    JSON = "json"
    XML = "xml"
    HTML = "html"
    TEXT = "text"


class FetchOutcome(StrEnum):
    """Why a poll produced the documents it did.

    NOT_MODIFIED and EMPTY are both successes with zero documents, and they mean very
    different things for freshness monitoring: a feed that 304s is healthy, a feed
    returning 200 with nothing new for six hours is the stale-but-successful failure
    SPEC §11 exists to catch. Collapsing them into "0 docs" loses that distinction.
    """

    OK = "ok"
    NOT_MODIFIED = "not_modified"
    EMPTY = "empty"
    ERROR = "error"


class BackfillHorizon(StrEnum):
    """How far back this source can be re-fetched after downtime. SPEC §3, §6.3.

    This is what makes catch-up honest. RSS cannot be backfilled beyond its feed
    window, so an outage means permanent loss and `source_health.gap_reason` has to
    say so rather than implying the interval was recovered.
    """

    COMPLETE = "complete"  # full history addressable (HN ids, FRED vintages)
    DAY = "day"  # roughly one day (EDGAR current feed)
    WINDOW = "window"  # only what is still in the feed (RSS)
    NONE = "none"


class RawDocument(BaseModel):
    """Exactly what a source returned, plus how it was obtained.

    Immutable by construction (SPEC §6.2: raw payloads are never overwritten). Fetch
    metadata is carried as first-class fields rather than a loose dict because §11's
    entire monitoring layer is built from these columns.
    """

    model_config = ConfigDict(frozen=True)

    ingest_id: str
    source_id: str
    fetched_at: datetime  # certain — when we received it
    source_url: str
    http_status: int
    outcome: FetchOutcome
    etag: str | None = None
    last_modified: str | None = None
    content_hash: str
    payload: bytes
    payload_format: PayloadFormat
    latency_ms: int
    byte_count: int

    @field_validator("fetched_at")
    @classmethod
    def _tz_aware_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("fetched_at must be timezone-aware")
        return v.astimezone(UTC)


class State(BaseModel):
    """Per-source pipeline state, persisted in DynamoDB. SPEC §6.2.

    Held small on purpose: this is a hot-path item read and written every 15 minutes,
    so `seen` is capped rather than unbounded (see `remember`).
    """

    source_id: str
    etag: str | None = None
    last_modified: str | None = None
    # A timestamp for time-ordered sources, or a sequence position (e.g. Hacker News's
    # highest fetched item id) for sources addressed by a dense integer range. SPEC §3.
    watermark: datetime | int | None = None
    seen: tuple[str, ...] = ()
    last_success_at: datetime | None = None
    consecutive_failures: int = 0

    SEEN_CAP: int = Field(default=5000, exclude=True)

    def remember(self, ids: list[str]) -> State:
        """Return new state with `ids` added, newest-first, capped.

        Capped because DynamoDB items are limited to 400 KB and this grows forever
        otherwise. Newest-first ordering means eviction drops the oldest ids, which are
        the ones least likely to reappear in a feed window.
        """
        merged = list(dict.fromkeys([*ids, *self.seen]))
        return self.model_copy(update={"seen": tuple(merged[: self.SEEN_CAP])})

    def has_seen(self, doc_id: str) -> bool:
        return doc_id in self.seen


class SourceConfig(BaseModel):
    """Static configuration for one source. Mirrors the Terraform sources map."""

    model_config = ConfigDict(frozen=True)

    source_id: str
    url: str
    payload_format: PayloadFormat
    backfill_horizon: BackfillHorizon
    freshness_sla_seconds: int
    min_docs_per_window: int = 0
    rate_limit_per_sec: float = 1.0
    user_agent: str = "signal/0.0 (+https://github.com/signal)"
    options: dict[str, Any] = Field(default_factory=dict)


class Poller(Protocol):
    """SPEC §6.1. The one interface a source must satisfy."""

    def __call__(self, config: SourceConfig, state: State) -> tuple[list[RawDocument], State]: ...
