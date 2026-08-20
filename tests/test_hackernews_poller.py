from __future__ import annotations

import httpx
import pytest
import respx

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, State
from signal_core.sources import hackernews


@pytest.fixture
def config():
    return SOURCES["hackernews"]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # The poller paces itself against rate_limit_per_sec between item fetches; tests
    # shouldn't actually wait for that.
    monkeypatch.setattr(hackernews.time, "sleep", lambda _seconds: None)


@respx.mock
def test_first_poll_starts_at_the_current_frontier_not_item_one(config):
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="100"))
    respx.get(f"{config.url}/item/100.json").mock(
        return_value=httpx.Response(200, json={"id": 100, "type": "story"})
    )

    documents, state = hackernews.poll(config, State(source_id=config.source_id))

    assert len(documents) == 1
    assert documents[0].outcome == FetchOutcome.OK
    assert documents[0].source_url.endswith("/item/100.json")
    assert state.watermark == 100


@respx.mock
def test_resumes_from_watermark_and_walks_the_range(config):
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="103"))
    for item_id in (101, 102, 103):
        respx.get(f"{config.url}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json={"id": item_id, "type": "story"})
        )

    documents, state = hackernews.poll(config, State(source_id=config.source_id, watermark=100))

    assert [d.source_url for d in documents] == [
        f"{config.url}/item/{i}.json" for i in (101, 102, 103)
    ]
    assert state.watermark == 103


@respx.mock
def test_a_deleted_item_is_empty_not_an_error(config):
    """A bare `null` body is a real outcome, never silently dropped. SPEC §6.2."""
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="1"))
    respx.get(f"{config.url}/item/1.json").mock(return_value=httpx.Response(200, text="null"))

    documents, _ = hackernews.poll(config, State(source_id=config.source_id))

    assert documents[0].outcome == FetchOutcome.EMPTY


@respx.mock
def test_one_items_failure_does_not_stop_the_others(config):
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="102"))
    respx.get(f"{config.url}/item/101.json").mock(return_value=httpx.Response(500))
    respx.get(f"{config.url}/item/102.json").mock(
        return_value=httpx.Response(200, json={"id": 102, "type": "story"})
    )

    documents, state = hackernews.poll(config, State(source_id=config.source_id, watermark=100))

    assert [d.outcome for d in documents] == [FetchOutcome.ERROR, FetchOutcome.OK]
    assert state.watermark == 102  # both ids were attempted; the watermark moves past both
    assert state.consecutive_failures == 0  # the poll as a whole still succeeded


@respx.mock
def test_maxitem_failure_increments_consecutive_failures_without_losing_watermark(config):
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(500))

    documents, state = hackernews.poll(
        config, State(source_id=config.source_id, watermark=100, consecutive_failures=1)
    )

    assert documents == []
    assert state.watermark == 100
    assert state.consecutive_failures == 2


@respx.mock
def test_caps_items_per_poll_to_bound_one_invocation(config, monkeypatch):
    monkeypatch.setattr(hackernews, "MAX_ITEMS_PER_POLL", 2)
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="1000"))
    for item_id in (101, 102):
        respx.get(f"{config.url}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json={"id": item_id})
        )

    documents, state = hackernews.poll(config, State(source_id=config.source_id, watermark=100))

    assert len(documents) == 2
    assert state.watermark == 102  # capped well short of maxitem=1000


# --- SPEC §11's content-movement signal, in this source's terms ---------------------


@respx.mock
def test_new_items_count_as_content_movement(config):
    """No body to hash here — a poll fetches many items, each unique by id — so the
    honest signal is whether HN produced anything new, i.e. did the watermark advance."""
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="101"))
    respx.get(f"{config.url}/item/101.json").mock(
        return_value=httpx.Response(200, text='{"id":101,"type":"story"}')
    )

    documents, state = hackernews.poll(config, State(source_id=config.source_id, watermark=100))

    assert len(documents) == 1
    assert state.last_content_change_at is not None


@respx.mock
def test_a_frozen_maxitem_does_not_advance_content_change(config):
    """HN reachable, returning 200s, and producing nothing new. The fetch is healthy and
    must keep reporting so; the content clock must not move, or an hour of a dead API
    reads exactly like an hour of a busy one."""
    respx.get(f"{config.url}/maxitem.json").mock(return_value=httpx.Response(200, text="100"))
    respx.get(f"{config.url}/item/100.json").mock(
        return_value=httpx.Response(200, text='{"id":100,"type":"story"}')
    )
    _, first = hackernews.poll(config, State(source_id=config.source_id, watermark=99))

    # maxitem stays put: nothing new exists to walk to.
    documents, second = hackernews.poll(config, first)

    assert documents == []
    assert second.last_success_at > first.last_success_at
    assert second.last_content_change_at == first.last_content_change_at
