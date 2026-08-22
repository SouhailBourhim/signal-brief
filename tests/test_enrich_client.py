"""Talking to Ollama. SPEC §7.3's determinism boundary; ADR-0003; 4B.B.

No test here reaches a real server — `respx` intercepts, the same way the poller tests do.
What is being defended is the shape of the request (which is what "temperature 0, pinned
digest" means in practice) and the failure behaviour (which is what stops a server being off
from looking like forty bad answers).
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from signal_core.config import Settings
from signal_core.enrich.client import (
    KEEP_ALIVE,
    OllamaUnavailable,
    generate,
    local_model_digest,
    supports_schema_format,
)

BASE = "http://localhost:11434"
SETTINGS = Settings(ollama_url=BASE, ollama_model="llama3.1:8b")


def _generate_route(**body):
    return respx.post(f"{BASE}/api/generate").mock(
        return_value=httpx.Response(200, json={"response": "{}", "model": "llama3.1:8b", **body})
    )


@respx.mock
def test_generation_is_requested_at_temperature_zero_with_a_fixed_seed():
    """§7.3's determinism boundary starts here. Temperature 0 is not a guarantee — GPU
    kernel scheduling is not bit-stable — which is exactly why SPEC §12's acceptance says
    enrichment "resolves from cache" rather than "reproduces identically"."""
    route = _generate_route()
    generate("prompt", settings=SETTINGS)

    sent = json.loads(route.calls[0].request.content)
    assert sent["options"]["temperature"] == 0
    assert sent["options"]["seed"] == 0
    assert sent["stream"] is False


@respx.mock
def test_the_configured_model_is_the_one_asked_for():
    """The pin is worthless if the request names a different tag than the digest describes."""
    route = _generate_route()
    generate("prompt", settings=SETTINGS)
    assert json.loads(route.calls[0].request.content)["model"] == "llama3.1:8b"


@respx.mock
def test_keep_alive_is_sent_so_a_batch_pays_the_model_load_once():
    """ADR-0003 measured a ~22.5 s load. Forty calls that each pay it is ~15 minutes instead
    of ~1, which is the difference between clearing the pre-brief window and not."""
    route = _generate_route()
    generate("prompt", settings=SETTINGS)
    assert json.loads(route.calls[0].request.content)["keep_alive"] == KEEP_ALIVE


@respx.mock
def test_a_schema_is_passed_as_format_when_the_caller_has_one():
    """Constrained decoding is the difference between "please return JSON" and "this is the
    only shape you can emit". Validation still runs on our side either way."""
    route = _generate_route()
    schema = {"type": "object", "properties": {"summary": {"type": "string"}}}
    generate("prompt", settings=SETTINGS, schema=schema)
    assert json.loads(route.calls[0].request.content)["format"] == schema


@respx.mock
def test_without_a_schema_it_falls_back_to_plain_json_mode():
    """Older Ollama builds accept only the string. The fallback has to still ask for JSON,
    or every answer becomes a quarantine row."""
    route = _generate_route()
    generate("prompt", settings=SETTINGS, schema=None)
    assert json.loads(route.calls[0].request.content)["format"] == "json"


@respx.mock
def test_a_connection_refusal_raises_the_operational_error_not_a_bad_answer():
    """The distinction the whole error type exists for: a server that is off is one
    operational fact, not forty validation failures. Quarantining it would bury the real
    rejects under noise about a service being down."""
    respx.post(f"{BASE}/api/generate").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(OllamaUnavailable):
        generate("prompt", settings=SETTINGS)


@respx.mock
def test_an_http_error_is_also_unavailable_rather_than_a_rejectable_answer():
    respx.post(f"{BASE}/api/generate").mock(return_value=httpx.Response(500, text="boom"))
    with pytest.raises(OllamaUnavailable):
        generate("prompt", settings=SETTINGS)


@respx.mock
def test_a_non_json_body_is_unavailable_rather_than_an_empty_generation():
    """A proxy returning an HTML error page must not read as "the model answered nothing"."""
    respx.post(f"{BASE}/api/generate").mock(return_value=httpx.Response(200, text="<html>"))
    with pytest.raises(OllamaUnavailable):
        generate("prompt", settings=SETTINGS)


@respx.mock
def test_timing_comes_back_so_the_dag_can_assert_the_capacity_bound():
    """§7.3 requires the DAG to assert its capacity bound and "fail loudly rather than
    silently lagging the 07:00 send". It needs a number to assert against."""
    _generate_route(eval_count=42)
    result = generate("prompt", settings=SETTINGS)
    assert result.elapsed_seconds >= 0
    assert result.eval_count == 42


# --- the pin ------------------------------------------------------------------------------


@respx.mock
def test_the_local_digest_is_read_for_the_configured_model_only():
    """ADR-0003's pin is a *measurement*. A box with three models installed must report the
    one the config names, not whichever the API listed first."""
    respx.get(f"{BASE}/api/tags").mock(
        return_value=httpx.Response(
            200,
            json={
                "models": [
                    {"name": "qwen3:4b", "digest": "sha256:wrong"},
                    {"name": "llama3.1:8b", "digest": "sha256:right"},
                ]
            },
        )
    )
    assert local_model_digest(SETTINGS) == "sha256:right"


@respx.mock
def test_an_unreachable_server_reports_no_digest_rather_than_a_stale_one():
    """Pinning a digest nobody verified is worse than `UNPINNED`: it makes the cache key look
    trustworthy while keying on a fiction."""
    respx.get(f"{BASE}/api/tags").mock(side_effect=httpx.ConnectError("refused"))
    assert local_model_digest(SETTINGS) is None


@respx.mock
def test_a_model_that_is_not_installed_reports_no_digest():
    respx.get(f"{BASE}/api/tags").mock(return_value=httpx.Response(200, json={"models": []}))
    assert local_model_digest(SETTINGS) is None


@respx.mock
def test_schema_support_is_probed_rather_than_inferred_from_a_version_string():
    """Ollama's version numbering has not tracked this capability cleanly, and the cost of
    asking is one tiny generation."""
    respx.post(f"{BASE}/api/generate").mock(
        return_value=httpx.Response(200, json={"response": '{"ok": true}'})
    )
    assert supports_schema_format(SETTINGS) is True


@respx.mock
def test_a_server_that_rejects_a_schema_format_reports_no_support():
    respx.post(f"{BASE}/api/generate").mock(return_value=httpx.Response(400, text="bad format"))
    assert supports_schema_format(SETTINGS) is False


@respx.mock
def test_an_unreachable_server_reports_no_schema_support_rather_than_raising():
    """The probe runs at the top of a batch. Raising here would turn "Ollama is off" into a
    crash instead of the graceful degradation the brief is built to survive."""
    respx.post(f"{BASE}/api/generate").mock(side_effect=httpx.ConnectError("refused"))
    assert supports_schema_format(SETTINGS) is False
