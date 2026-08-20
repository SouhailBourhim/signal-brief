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
    # And requires it plainly. A browser-style token or a URL in parentheses gets a 403
    # from EDGAR — "Your Request Originates from an Undeclared Automated Tool" — which
    # this project learned the expensive way. See Settings.user_agent.
    assert "(" not in sent_ua and "http" not in sent_ua and "/" not in sent_ua


# --- SPEC §11's content-movement signal (docs/decisions/ADR-0009) -------------------


@respx.mock
def test_first_poll_counts_as_content_movement(config):
    """No prior content to be identical to. Seeding this as "unchanged" would report
    every brand-new source as a dead feed until its second distinct body arrived."""
    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>one</rss>"))

    _, state = rss_tech.poll(config, State(source_id=config.source_id))

    assert state.last_content_change_at is not None
    assert state.last_content_hash is not None


@respx.mock
def test_an_unchanged_200_body_does_not_advance_content_change(config):
    """The dead-feed case, and the one a status code cannot reveal: both SEC sources
    serve no validators, so a frozen feed 200s a byte-identical body forever."""
    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>same</rss>"))

    _, first = rss_tech.poll(config, State(source_id=config.source_id))
    _, second = rss_tech.poll(config, first)

    # The fetch is healthy and keeps advancing...
    assert second.last_success_at > first.last_success_at
    # ...while the content has demonstrably not moved.
    assert second.last_content_change_at == first.last_content_change_at


@respx.mock
def test_a_changed_200_body_advances_content_change(config):
    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>one</rss>"))
    _, first = rss_tech.poll(config, State(source_id=config.source_id))

    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>two</rss>"))
    _, second = rss_tech.poll(config, first)

    assert second.last_content_change_at > first.last_content_change_at
    assert second.last_content_hash != first.last_content_hash


@respx.mock
def test_a_304_seeds_content_change_when_it_has_never_been_set(config):
    """A source that 304s from its very first poll — `rss_ars` does — would otherwise hold
    None forever, and `assess_source` reads None as "skip the dead-feed check". The
    lowest-volume source would have been the one exempt from dead-feed detection.

    Found in production on 2026-08-20, not in tests: five of six sources had the field set
    within minutes of deploy and `rss_ars` never did."""
    respx.get(config.url).mock(return_value=httpx.Response(304))

    _, state = rss_tech.poll(config, State(source_id=config.source_id, etag='"v1"'))

    assert state.last_content_change_at is not None


@respx.mock
def test_a_304_does_not_advance_content_change(config):
    """A 304 is the server stating outright that nothing moved — the cleanest possible
    evidence for the check, and previously the clearest way to look healthy while dead."""
    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>one</rss>"))
    _, first = rss_tech.poll(config, State(source_id=config.source_id))

    respx.get(config.url).mock(return_value=httpx.Response(304))
    _, second = rss_tech.poll(config, first)

    assert second.last_success_at > first.last_success_at
    assert second.last_content_change_at == first.last_content_change_at


@respx.mock
def test_a_failed_fetch_does_not_advance_content_change(config):
    """An error says nothing about whether the publisher is alive, so it must not reset
    the dead-feed clock — that would let a flapping source mask a frozen one."""
    respx.get(config.url).mock(return_value=httpx.Response(200, content=b"<rss>one</rss>"))
    _, first = rss_tech.poll(config, State(source_id=config.source_id))

    respx.get(config.url).mock(return_value=httpx.Response(503, content=b"nope"))
    _, second = rss_tech.poll(config, first)

    assert second.consecutive_failures == 1
    assert second.last_content_change_at == first.last_content_change_at
