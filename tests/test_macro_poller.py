"""The ALFRED poller and its parser. SPEC §8; 4B.H.

`respx` intercepts; no test reaches FRED and none needs a key. What is defended is the poll
contract (fetch bytes, interpret nothing, never raise), the secret handling, and the three
payload details that are easy to get wrong — the string values, the `"."` sentinel, and the
`9999-12-31` "still current" marker.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, State
from signal_core.parse.macro import parse, series_id_from_url
from signal_core.sources.macro import PATH, PLACEHOLDER, VINTAGE_WINDOW_YEARS, _realtime_start, poll

FIXTURES = Path(__file__).parent / "fixtures" / "bronze" / "macro"
PAYEMS = (FIXTURES / "payems_vintages.json").read_bytes()
NO_KEY = (FIXTURES / "no_api_key.json").read_bytes()

BASE = SOURCES["macro"].url
CONFIG = SOURCES["macro"].model_copy(update={"options": {"api_key": "test-key"}})
STATE = State(source_id="macro")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Six series at 0.5s each would add three seconds to every test in this file."""
    monkeypatch.setattr("signal_core.sources.macro.time.sleep", lambda _: None)


def _route(*, payload: bytes = PAYEMS, status: int = 200):
    return respx.get(f"{BASE}{PATH}").mock(return_value=httpx.Response(status, content=payload))


# --- the poll contract --------------------------------------------------------------------


@respx.mock
def test_one_document_per_watchlist_series():
    """SPEC §3's extensibility claim rests on the watchlist being the only place a series is
    named — editing `watchlist.toml` must change tomorrow's fetch with no redeploy."""
    from signal_core.watchlist import load

    _route()
    documents, _ = poll(CONFIG, STATE)

    assert len(documents) == len(load().macro_series)
    assert {d.outcome for d in documents} == {FetchOutcome.OK}


@respx.mock
def test_the_api_key_never_reaches_the_stored_url():
    """Bronze is immutable (SPEC §6.2). A secret written into it cannot be redacted later —
    only the whole object deleted — so the URL is reconstructed from the parameters that
    matter rather than copied off the request."""
    _route()
    documents, _ = poll(CONFIG, STATE)

    for document in documents:
        assert "test-key" not in document.source_url
        assert "api_key" not in document.source_url


@respx.mock
def test_the_series_id_survives_into_the_stored_url():
    """FRED does not echo the series id in the body, so `spark/jobs/macro.py` reads it back
    out of `source_url`. If it were dropped here, six series would merge into one."""
    _route()
    documents, _ = poll(CONFIG, STATE)

    recovered = {series_id_from_url(d.source_url) for d in documents}
    assert "" not in recovered
    assert "PAYEMS" in recovered


@respx.mock
def test_an_http_error_becomes_an_error_document_not_an_exception():
    """SPEC §6.1: a failed fetch is an ERROR document. An escaped exception means
    infrastructure broke, which is what the CloudWatch alarms are for — a bad key is not
    that."""
    _route(payload=NO_KEY, status=400)
    documents, state = poll(CONFIG, STATE)

    assert {d.outcome for d in documents} == {FetchOutcome.ERROR}
    assert state.consecutive_failures == 1


@respx.mock
def test_fred_s_own_reason_is_stored_as_the_payload():
    """A 400 body names what is wrong. Keeping it makes the failure diagnosable straight out
    of bronze without re-running anything."""
    _route(payload=NO_KEY, status=400)
    documents, _ = poll(CONFIG, STATE)

    assert b"api_key is not set" in documents[0].payload


@respx.mock
def test_a_transport_failure_is_an_error_document_too():
    respx.get(f"{BASE}{PATH}").mock(side_effect=httpx.ConnectError("refused"))
    documents, state = poll(CONFIG, STATE)

    assert {d.outcome for d in documents} == {FetchOutcome.ERROR}
    assert all(d.http_status == 0 for d in documents)
    assert state.consecutive_failures == 1


@respx.mock
def test_content_movement_is_measured_over_all_series_together():
    """`State` holds one content hash, and the honest question for this source is "did any
    vintage anywhere move" — not "did PAYEMS move"."""
    _route()
    _, first = poll(CONFIG, STATE)
    _, second = poll(CONFIG, first)

    assert first.last_content_hash is not None
    assert second.last_content_hash == first.last_content_hash
    assert second.last_content_change_at == first.last_content_change_at, (
        "identical payloads must not advance the content clock — that is the dead-feed signal"
    )


@respx.mock
def test_a_changed_vintage_advances_the_content_clock():
    """`assess_source`'s `dead_feed` check is the only thing that can see a source returning
    200 with content that never moves (SPEC §11, 1.E)."""
    _route()
    _, first = poll(CONFIG, STATE)

    respx.get(f"{BASE}{PATH}").mock(
        return_value=httpx.Response(200, content=PAYEMS.replace(b"159218", b"159100"))
    )
    _, second = poll(CONFIG, first)

    assert second.last_content_hash != first.last_content_hash
    assert second.last_content_change_at != first.last_content_change_at


# --- the vintage window --------------------------------------------------------------------
#
# Regression coverage for the real failure found against the live account (2026-08-23): a
# fixed REALTIME_START="2015-01-01" made DFF and DGS10 fail with FRED's 2,000-vintage-date
# cap, because they are daily series and a monthly-series-sized window blows past it.


def test_the_window_is_measured_backward_from_now_not_a_fixed_date():
    """A fixed calendar anchor only ages toward FRED's cap; a rolling one holds a constant
    margin forever. `ops/recovery.py`'s backfill horizon makes the identical argument for the
    identical reason."""
    from datetime import UTC, datetime

    earlier = _realtime_start(datetime(2020, 1, 1, tzinfo=UTC))
    later = _realtime_start(datetime(2026, 1, 1, tzinfo=UTC))
    assert earlier != later, "the window did not move when `now` did"
    assert earlier < later


def test_the_window_is_the_stated_number_of_years():
    from datetime import UTC, datetime

    now = datetime(2026, 8, 23, tzinfo=UTC)
    start = _realtime_start(now)
    assert start.startswith(str(now.year - VINTAGE_WINDOW_YEARS))


@respx.mock
def test_every_series_in_one_poll_shares_the_same_window():
    """Computed once per `poll()` call, not once per series — otherwise a poll straddling
    midnight could send six requests with six different windows for no reason."""
    route = _route()
    poll(CONFIG, STATE)

    starts = {dict(r.request.url.params)["realtime_start"] for r in route.calls}
    assert len(starts) == 1, "the six requests in one poll used different windows"


@respx.mock
def test_the_stored_url_names_the_window_actually_requested():
    """SPEC §6.1: the record must not lie about what was fetched. Bronze is immutable, so a
    stored URL claiming a different window than the one actually sent would be permanently
    wrong with no way to correct it short of deleting the row."""
    route = _route()
    documents, _ = poll(CONFIG, STATE)

    sent_start = dict(route.calls[0].request.url.params)["realtime_start"]
    assert f"realtime_start={sent_start}" in documents[0].source_url


# --- the secret ---------------------------------------------------------------------------


@respx.mock
def test_the_terraform_placeholder_is_named_rather_than_sent_to_fred():
    """The expected state between `terraform apply` and the manual put-parameter. Sending
    `UNSET` as a key would produce FRED's generic rejection instead of the one sentence that
    tells the reader what to do — the same courtesy `mail.tf` extends for SES."""
    from signal_core.sources import macro

    macro._API_KEY_CACHE.clear()
    config = SOURCES["macro"].model_copy(
        update={"options": {"api_key": "", "api_key_parameter": "/signal/fred-api-key"}}
    )

    def _fake_ssm(**_):
        return {"Parameter": {"Value": PLACEHOLDER}}

    class _Client:
        get_parameter = staticmethod(_fake_ssm)

    import sys
    import types

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda _name: _Client()  # type: ignore[attr-defined]
    sys.modules["boto3"] = fake_boto3
    try:
        documents, state = poll(config, STATE)
    finally:
        sys.modules.pop("boto3", None)
        macro._API_KEY_CACHE.clear()

    assert {d.outcome for d in documents} == {FetchOutcome.ERROR}
    assert b"Terraform placeholder" in documents[0].payload
    assert state.consecutive_failures == 1


def test_a_missing_parameter_name_is_an_error_document_not_a_crash():
    config = SOURCES["macro"].model_copy(update={"options": {}})
    documents, state = poll(config, STATE)

    assert {d.outcome for d in documents} == {FetchOutcome.ERROR}
    assert b"api_key_parameter" in documents[0].payload
    assert state.consecutive_failures == 1


def test_one_error_document_per_series_not_one_for_the_batch():
    """`ops.source_health` counts documents. A single row would read as "the source produced
    one document today", which is a different and less alarming fact than "all six failed"."""
    from signal_core.watchlist import load

    config = SOURCES["macro"].model_copy(update={"options": {}})
    documents, _ = poll(config, STATE)
    assert len(documents) == len(load().macro_series)


# --- the parser ---------------------------------------------------------------------------


def test_every_vintage_of_a_revised_period_is_kept():
    """The whole of SPEC §8. A pipeline that kept only the newest value would destroy the
    fact the brief exists to state."""
    result = parse(PAYEMS)

    may = [o for o in result.macro_observations if o.period == date(2026, 5, 1)]
    assert len(may) == 3
    assert [o.value for o in may] == [159310.0, 159264.0, 159218.0]


def test_the_still_current_sentinel_becomes_none_rather_than_the_year_9999():
    """A sentinel that survives into the table is one someone eventually does arithmetic on."""
    result = parse(PAYEMS)

    current = [o for o in result.macro_observations if o.superseded_at is None]
    superseded = [o for o in result.macro_observations if o.superseded_at is not None]

    assert current, "nothing marked current"
    assert all(o.superseded_at != date(9999, 12, 31) for o in result.macro_observations)
    assert superseded[0].superseded_at == date(2026, 7, 2)


def test_freds_missing_marker_becomes_null_not_zero():
    """A zero unemployment rate is a very different claim from an unpublished one, and a
    `revision_delta` computed against a zero-filled gap reports a fictional revision the size
    of the whole series."""
    result = parse(PAYEMS)

    august = [o for o in result.macro_observations if o.period == date(2026, 8, 1)]
    assert len(august) == 1
    assert august[0].value is None


def test_values_arrive_as_strings_and_come_out_as_floats():
    result = parse(PAYEMS)
    assert all(isinstance(o.value, float) for o in result.macro_observations if o.value is not None)


def test_freds_in_band_error_is_a_row_level_reject_not_an_empty_series():
    """FRED reports a bad key with a JSON body, not only a status code. Reading it as "this
    series has no observations" would look identical to a quiet month."""
    result = parse(NO_KEY)
    assert result.error is not None
    assert "api_key" in result.error
    assert result.macro_observations == []


def test_a_truncated_response_is_reported_rather_than_trusted():
    """FRED caps a response at `limit`, and a truncated series looks like one that simply
    stops. Silence here would be indistinguishable from a series that ended."""
    document = json.loads(PAYEMS)
    document["count"] = 100_000
    result = parse(json.dumps(document).encode())

    assert any("truncated" in w for w in result.warnings)
    assert result.macro_observations, "the observations that did arrive are still usable"


def test_an_observation_missing_a_time_axis_is_skipped_and_counted():
    """Both axes are the record. An observation missing either is unplaceable, and defaulting
    it onto today's date would make it look like a fresh vintage."""
    document = json.loads(PAYEMS)
    document["observations"].append({"date": "2026-09-01", "value": "1"})  # no realtime_start
    result = parse(json.dumps(document).encode())

    assert any("skipped" in w for w in result.warnings)
    assert all(o.vintage_date is not None for o in result.macro_observations)


def test_malformed_json_is_a_parse_error_not_a_crash():
    assert parse(b"<html>not json</html>").error is not None


def test_the_series_id_is_left_to_the_spark_job():
    """Keeping `parse` a pure function of the payload is what makes it testable against a
    committed fixture — the id is a property of the request, not the response."""
    result = parse(PAYEMS)
    assert all(o.series_id == "" for o in result.macro_observations)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://api.stlouisfed.org/fred/series/observations?series_id=PAYEMS&x=1", "PAYEMS"),
        ("https://api.stlouisfed.org/fred/series/observations?x=1", ""),
        ("", ""),
        (None, ""),
    ],
)
def test_series_id_is_recovered_from_the_stored_url(url, expected):
    assert series_id_from_url(url) == expected
