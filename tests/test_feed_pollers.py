from __future__ import annotations

import httpx
import pytest
import respx

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, State
from signal_core.sources import edgar, rss_tech


@pytest.fixture
def config():
    return SOURCES["rss_tech"]


@respx.mock
def test_first_poll_stores_the_whole_feed_and_advances_etag(config):
    respx.get(config.url).mock(
        return_value=httpx.Response(200, headers={"ETag": '"v1"'}, content=b"<rss>...</rss>")
    )

    documents, state = rss_tech.poll(config, State(source_id=config.source_id))

    assert len(documents) == 1
    assert documents[0].outcome == FetchOutcome.OK
    assert documents[0].payload == b"<rss>...</rss>"
    assert state.etag == '"v1"'
    assert state.consecutive_failures == 0


@respx.mock
def test_conditional_get_sends_the_stored_etag_and_last_modified(config):
    route = respx.get(config.url).mock(return_value=httpx.Response(304))

    state = State(
        source_id=config.source_id, etag='"v1"', last_modified="Tue, 18 Aug 2026 07:00:00 GMT"
    )
    documents, state = rss_tech.poll(config, state)

    assert documents == []
    assert state.etag == '"v1"'  # unchanged — SPEC §6.2, most polls should land here
    sent = route.calls.last.request
    assert sent.headers["if-none-match"] == '"v1"'
    assert sent.headers["if-modified-since"] == "Tue, 18 Aug 2026 07:00:00 GMT"


@respx.mock
def test_http_error_is_quarantined_not_raised(config):
    respx.get(config.url).mock(return_value=httpx.Response(500, text="boom"))

    documents, state = rss_tech.poll(config, State(source_id=config.source_id, etag='"v1"'))

    assert len(documents) == 1
    assert documents[0].outcome == FetchOutcome.ERROR
    assert documents[0].http_status == 500
    assert state.consecutive_failures == 1
    assert state.etag == '"v1"'  # last good etag preserved, SPEC §6.2


@respx.mock
def test_connection_error_is_quarantined_too(config):
    respx.get(config.url).mock(side_effect=httpx.ConnectError("refused"))

    documents, state = rss_tech.poll(config, State(source_id=config.source_id))

    assert documents[0].outcome == FetchOutcome.ERROR
    assert documents[0].http_status == 0
    assert state.consecutive_failures == 1


@respx.mock
def test_edgar_sends_a_descriptive_user_agent_with_contact_email():
    config = SOURCES["edgar"]
    route = respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<feed/>"))

    edgar.poll(config, State(source_id=config.source_id))

    sent_ua = route.calls.last.request.headers["user-agent"]
    assert "@" in sent_ua  # SEC requires a contact email in the User-Agent, SPEC §6.2
