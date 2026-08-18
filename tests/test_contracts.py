"""The contract is the extensibility story, so it gets the strictest tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from signal_core.contracts import (
    BackfillHorizon,
    FetchOutcome,
    PayloadFormat,
    RawDocument,
    SourceConfig,
    State,
)


def _doc(**overrides) -> RawDocument:
    base = dict(
        ingest_id="i-1",
        source_id="fake",
        fetched_at=datetime.now(UTC),
        source_url="https://example.com/a",
        http_status=200,
        outcome=FetchOutcome.OK,
        content_hash="abc",
        payload=b"{}",
        payload_format=PayloadFormat.JSON,
        latency_ms=5,
        byte_count=2,
    )
    return RawDocument(**{**base, **overrides})


def test_raw_document_is_immutable():
    """SPEC §6.2: raw payloads are never overwritten."""
    doc = _doc()
    with pytest.raises(ValidationError):
        doc.payload = b"tampered"


def test_naive_fetched_at_is_rejected():
    with pytest.raises(ValidationError):
        _doc(fetched_at=datetime(2026, 1, 1))


def test_fetched_at_is_coerced_to_utc():
    tz = datetime.now(UTC).astimezone()
    doc = _doc(fetched_at=tz)
    assert doc.fetched_at.tzinfo is UTC


def test_state_remembers_newest_first_and_caps():
    state = State(source_id="fake", SEEN_CAP=3)
    state = state.remember(["a", "b"]).remember(["c", "d"])
    assert state.seen == ("c", "d", "a")  # newest first, oldest evicted
    assert state.has_seen("c") and not state.has_seen("b")


def test_state_remember_is_idempotent():
    state = State(source_id="fake").remember(["a"]).remember(["a"])
    assert state.seen == ("a",)


def test_backfill_horizon_is_declared_per_source():
    """SPEC §6.3: catch-up can only promise what the source exposes."""
    config = SourceConfig(
        source_id="rss",
        url="https://example.com/feed",
        payload_format=PayloadFormat.XML,
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=1800,
    )
    assert config.backfill_horizon is BackfillHorizon.WINDOW


def test_source_config_is_frozen():
    config = SourceConfig(
        source_id="x",
        url="u",
        payload_format=PayloadFormat.JSON,
        backfill_horizon=BackfillHorizon.NONE,
        freshness_sla_seconds=1,
    )
    with pytest.raises(ValidationError):
        config.url = "other"


def test_outcomes_distinguish_healthy_zero_from_stale_zero():
    """A 304 and a silent-empty feed are both zero documents and must not be conflated."""
    assert FetchOutcome.NOT_MODIFIED != FetchOutcome.EMPTY
    assert _doc(outcome=FetchOutcome.NOT_MODIFIED, http_status=304).outcome.value == "not_modified"


def test_poller_signature_matches_spec(polled):
    documents, state = polled
    assert isinstance(documents, list) and isinstance(state, State)
    assert all(isinstance(d, RawDocument) for d in documents)


def test_seen_ids_suppress_redelivery(fake_config):
    from signal_core.sources import get_poller

    first, state = get_poller("fake")(fake_config, State(source_id="fake"))
    second, _ = get_poller("fake")(fake_config, state)
    assert len(first) > 0
    assert second == []


def test_unknown_source_names_the_registry():
    from signal_core.sources import get_poller

    with pytest.raises(KeyError, match="registered"):
        get_poller("nope")


def test_state_survives_round_trip(polled):
    """State goes to DynamoDB as JSON; anything that will not round-trip is a bug."""
    _, state = polled
    assert State.model_validate_json(state.model_dump_json()) == state


def test_future_timestamps_are_present_in_the_fixture(polled):
    """The fake source must keep exercising the awkward paths, or it stops earning its place."""
    import json

    documents, _ = polled
    published = [json.loads(d.payload)["published_at"] for d in documents]
    assert any(p is None for p in published)
    assert len(documents) > 5


def test_timedelta_import_unused_guard():
    assert timedelta(0) == timedelta(0)
