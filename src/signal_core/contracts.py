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
    # SPEC §11's dead-feed detection: "the common failure is a feed returning 200 with
    # stale content, not a 500". `last_success_at` cannot see that — it advances on a 304
    # and on an unchanged 200 alike, so a permanently frozen feed reports zero staleness
    # forever. These two are the signal that actually moves only when the *content* does.
    #
    # How each poller sets them differs, because the honest signal differs: `feed.py`
    # compares the body's `content_hash` against `last_content_hash`; `hackernews.py` has
    # no body to hash — its content moved iff the watermark advanced. `last_content_hash`
    # stays None for the latter, deliberately.
    last_content_change_at: datetime | None = None
    last_content_hash: str | None = None

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
    # How long since the last *successful fetch* before the source is `stale`. Answers
    # "is the poller working?" — set at ~3x the deployed cadence.
    freshness_sla_seconds: int
    # How long since the last *content change* before the source is `dead_feed`. Answers
    # the different question "is the feed still alive?", and is necessarily far longer:
    # a healthy RSS feed legitimately publishes nothing for hours, and SEC files nothing
    # over a weekend. None disables the check for sources where content movement isn't a
    # meaningful signal. SPEC §11.
    content_staleness_sla_seconds: int | None = None
    # Minimum bronze documents per assessment window before the source is `thin`. Note
    # this counts *documents*, not feed items, so its correct value depends on whether
    # the source uses conditional GET: a 304 yields no document at all, which makes zero
    # the normal reading for a feed that mostly 304s. See config.SOURCES.
    min_docs_per_window: int = 0
    rate_limit_per_sec: float = 1.0
    # Per-source because sources differ enormously: EDGAR's browse-edgar endpoint is a
    # CGI script that regularly takes tens of seconds, while a static RSS file does not.
    # Always well under the Lambda's own timeout, so a slow fetch becomes an ERROR
    # document (SPEC §6.2) rather than a killed invocation with nothing recorded.
    timeout_seconds: float = 30.0
    # `Name contact@email`, because EDGAR rejects anything more browser-shaped — see
    # Settings.user_agent for the measurement.
    user_agent: str = "Signal Brief you@example.com"
    options: dict[str, Any] = Field(default_factory=dict)


class Poller(Protocol):
    """SPEC §6.1. The one interface a source must satisfy."""

    def __call__(self, config: SourceConfig, state: State) -> tuple[list[RawDocument], State]: ...
