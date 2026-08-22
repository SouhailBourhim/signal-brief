"""Talking to Ollama. SPEC §7.3's determinism boundary; ADR-0002, ADR-0003.

Ollama runs native on the host rather than in Compose (ADR-0002: the GPU is why inference is
free, and reaching it from a container needs the NVIDIA toolkit for nothing gained), so this
is a plain HTTP client against `Settings.ollama_url` with no container plumbing.

## What "determinism boundary" means concretely here

SPEC §7.3 asks for "temperature 0, pinned model digest, versioned prompts, and everything
downstream marked non-reproducible in lineage." The first three are enforced in this module;
the fourth is a property of the tables, not of the client. Temperature 0 and a fixed seed
make the model *as* deterministic as it can be made — which is not fully, because kernel
scheduling on a GPU is not bit-stable across runs. **That is exactly why SPEC §12's
acceptance says enrichment "resolves from cache" rather than "reproduces identically."** The
cache is the reproducibility mechanism; the model is not, and this docstring is here so that
nobody later mistakes temperature 0 for a determinism guarantee.

## Structured output

Modern Ollama accepts a JSON Schema as `format` and constrains decoding to it; older builds
accept only the string `"json"`, which asks for valid JSON and promises nothing about shape.
`supports_schema_format` probes the running server rather than assuming, and the caller falls
back to `"json"` plus the schema spelled out in the prompt. Either way the output goes
through `Enrichment.model_validate_json`, so the schema is enforced on this side regardless
of what the server did — the difference is how often that enforcement has to reject.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import httpx

from signal_core.config import Settings

# Long enough that a batch of heads pays ADR-0003's ~22.5 s model load once rather than once
# per call. ADR-0003 measured the difference: ~1 minute for a 40-head batch that amortizes one
# load, against 86 s for the same 40 calls made cold-ish. Ollama's own default is 5 minutes.
KEEP_ALIVE = "10m"

# `num_predict` bounds one response. The schema's summary ceiling is 400 characters and the
# extraction object is small, so a well-behaved answer is well under this; the bound exists to
# stop a degenerate repeat loop from consuming a batch's entire budget.
NUM_PREDICT = 300

# Fixed so that a re-run under the same model and prompt has the best chance of reproducing.
# Not a guarantee — see the module docstring.
SEED = 0


class OllamaUnavailable(RuntimeError):
    """The server is not reachable or did not answer.

    A distinct type because the callers treat it differently from a bad *answer*: a model
    that returns unparseable JSON is a quarantine case (SPEC §7.3), while a server that is
    off is an operational fact the brief should degrade around rather than quarantine
    thousands of rejects over.
    """


@dataclass(frozen=True)
class Generation:
    """One raw model response, before validation. Timing is carried because §7.3's capacity
    paragraph is a claim the enrich DAG has to be able to assert against."""

    text: str
    elapsed_seconds: float
    model: str
    eval_count: int | None = None


def _client(settings: Settings, timeout: float) -> httpx.Client:
    return httpx.Client(base_url=settings.ollama_url.rstrip("/"), timeout=timeout)


def local_model_digest(settings: Settings | None = None, *, timeout: float = 10.0) -> str | None:
    """The digest of the locally installed model, or None if the server is unreachable.

    This is what makes ADR-0003's pin a *measurement* rather than a value copied out of a
    document. The ADR recorded a digest on 2026-08-18; Ollama may have re-pulled the tag
    since, and pinning a digest nobody verified is worse than leaving it `UNPINNED`, because
    it makes the cache key look trustworthy while keying on a fiction.
    """
    settings = settings or Settings()
    try:
        with _client(settings, timeout) as client:
            response = client.get("/api/tags")
            response.raise_for_status()
            for model in response.json().get("models", []):
                if model.get("name") == settings.ollama_model:
                    return model.get("digest")
    except (httpx.HTTPError, ValueError):
        return None
    return None


def supports_schema_format(settings: Settings | None = None, *, timeout: float = 30.0) -> bool:
    """Whether this server constrains decoding to a supplied JSON Schema.

    Probed, not inferred from a version string: Ollama's version numbering has not tracked
    this capability cleanly, and the cost of asking is one tiny generation.
    """
    settings = settings or Settings()
    probe = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
    try:
        with _client(settings, timeout) as client:
            response = client.post(
                "/api/generate",
                json={
                    "model": settings.ollama_model,
                    "prompt": 'Answer with {"ok": true}',
                    "format": probe,
                    "stream": False,
                    "options": {"temperature": 0, "num_predict": 20},
                },
            )
            if response.status_code != httpx.codes.OK:
                return False
            return "ok" in json.loads(response.json().get("response", "{}"))
    except (httpx.HTTPError, ValueError, KeyError):
        return False


def generate(
    prompt: str,
    *,
    settings: Settings | None = None,
    schema: dict[str, Any] | None = None,
    timeout: float = 120.0,
) -> Generation:
    """One completion at temperature 0.

    `schema` constrains decoding when the server supports it; pass None to fall back to
    `format: "json"`. Either way the answer is validated on this side.
    """
    settings = settings or Settings()
    payload: dict[str, Any] = {
        "model": settings.ollama_model,
        "prompt": prompt,
        "format": schema if schema is not None else "json",
        "stream": False,
        "keep_alive": KEEP_ALIVE,
        "options": {"temperature": 0, "seed": SEED, "num_predict": NUM_PREDICT},
    }
    started = time.monotonic()
    try:
        with _client(settings, timeout) as client:
            response = client.post("/api/generate", json=payload)
            response.raise_for_status()
            body = response.json()
    except httpx.HTTPError as exc:
        raise OllamaUnavailable(f"{settings.ollama_url}: {exc}") from exc
    except ValueError as exc:
        raise OllamaUnavailable(f"{settings.ollama_url}: non-JSON response") from exc

    return Generation(
        text=body.get("response", ""),
        elapsed_seconds=time.monotonic() - started,
        model=body.get("model", settings.ollama_model),
        eval_count=body.get("eval_count"),
    )
