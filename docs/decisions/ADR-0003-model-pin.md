# ADR-0003 — Local model selection and pinning

**Status:** Accepted · **Date:** 2026-08-18

## Context

SPEC §7.3 requires a pinned model digest, versioned prompts, temperature 0, and a
determinism boundary marked in lineage. It also assumes **~4 seconds per cluster**, which
was a CPU-era estimate written before the target hardware was known.

## Decision (pending measurement)

Run an 8B-class model at q4 (~4.7 GB, fits 8 GB VRAM with context headroom) via Ollama on
the host, pinned by digest in `signal_core.config.Settings.ollama_model_digest`, which
ships as `UNPINNED` and must be replaced before Phase 4.

## Measurement (dev box, RTX 5070 8GB, 2026-08-18)

- **Model:** `llama3.1:8b`, quantization `Q4_K_M`, 8.0B params, 4.9 GB on disk.
- **Digest:** `sha256:667b0c1932bc6ffc593ed1d03f895bf2dc8dc6df21db3042284a6f4416b06a29`
  (`ollama show llama3.1:8b --modelfile`, the blob behind `FROM`).
- **Method:** 40 sequential calls to `/api/generate` at `temperature=0`, cycling 7
  representative cluster-head prompts (headline + body, asking for a one-sentence
  summary, topic classification, and an entity list — the combined shape of SPEC
  §7.3's three enrichment jobs), `num_predict=200`.
- **Tokens/sec:** 53-70 tok/s eval rate, averaging **~60 tok/s**; the range reflects
  GPU clocking up over the run, not prompt variance — later calls are consistently
  faster than the first few.
- **Wall-clock:** one-time model load is **~22.5 s** (pays once per `keep_alive`
  window, default 5 min); steady-state generation after that averages **~1.0 s per
  head** (34-55 output tokens each). A realistic 40-head pre-brief batch run
  back-to-back is therefore **~23 s load + ~40 s generation ≈ 1 minute**, not the ~3
  minutes SPEC §7.3 assumed. Measured directly (40 calls, cold start included):
  86.0 s total, but that run made 40 independent cold-ish calls rather than one
  batch amortizing a single load — the ~1 minute figure is the correct estimate for
  the actual pre-brief access pattern (one load, 40 generations).

## Decision made

Pin `ollama_model_digest = "sha256:667b0c1932bc6ffc593ed1d03f895bf2dc8dc6df21db3042284a6f4416b06a29"`
before Phase 4 starts (still `UNPINNED` in code as of Phase 0 — this ADR records the
value to pin, the config change lands with Phase 4 enrichment work).

## Consequences

- SPEC §7.3's capacity paragraph is rewritten with this measurement (see commit).
- Until `signal_core.config.Settings.ollama_model_digest` is actually set to the pinned
  value above, the enrichment cache key is unstable and cache-hit rate is not a
  meaningful metric — which is why Phase 4 cannot start before that lands.

## Re-verified 2026-08-23 — the digest has not drifted

Read straight off the installed manifest
(`~/.ollama/models/manifests/registry.ollama.ai/library/llama3.1/8b`) rather than from a
running server, because the server was still down:

    "mediaType": "application/vnd.ollama.image.model",
    "digest": "sha256:667b0c1932bc6ffc593ed1d03f895bf2dc8dc6df21db3042284a6f4416b06a29",
    "size": 4920738944

**Byte-identical to the value recorded above on 2026-08-18.** The tag has not been re-pulled,
so the pin below is still the right one to set.

### Two digests, and only one of them is the model

Reading that manifest caught a bug in `enrich/client.py::local_model_digest` before its first
real use. Ollama exposes **two different digests** for one model:

| Source | What it is |
|---|---|
| `/api/tags` → `models[].digest` | the **manifest** digest — a hash of the JSON above, listing four layers |
| `/api/show` → `modelfile`'s `FROM` line | the **model blob** — `application/vnd.ollama.image.model`, the weights |

This ADR recorded the second, via `ollama show --modelfile`. The first implementation read the
first. Comparing them makes `signal enrich --check-model` report drift on a box where nothing
has drifted — **which is worse than not checking at all**, because it trains the operator to
ignore the one signal that says the cache key has stopped meaning anything.

Fixed to read `/api/show` and parse the `FROM` blob path, with
`test_the_digest_is_the_model_blob_not_the_manifest` pinning it. Worth recording rather than
just fixing: the two values look equally digest-shaped, and nothing about either name says
which describes the weights.

## Still `UNPINNED` as of 2026-08-22 — and the pin is a *measurement*, not a copy

Phase 4B's enrichment stage is built, but the digest above was **not** copied into the config,
because Ollama was not running on the dev box when 4B was written (`localhost`, `127.0.0.1`,
the WSL2 gateway `172.18.240.1` and `host.docker.internal` all refused on :11434).

That is deliberate rather than an oversight. This ADR recorded a digest on 2026-08-18, and
Ollama may have re-pulled the `llama3.1:8b` tag since — the tag floats, the digest does not.
**Pinning a digest nobody verified against the running box is worse than leaving it
`UNPINNED`**, because it makes the cache key look trustworthy while keying on a fiction, and
every row written under it would be a cache entry that can never legitimately be served.

`signal enrich --check-model` is the verification step. It exits 0 when the pin matches the
installed model, 1 when it disagrees or is unset, and 2 when the server is unreachable —
because "could not tell" and "wrong" are different answers and collapsing them would let an
unreachable server read as a pass. `cli_enrich.run_enrich` refuses to run at all while the
digest is `UNPINNED`.

If the digest has drifted, record it here as a **second measurement with its date** rather
than silently repinning, and note that every cached enrichment under the old digest becomes
correctly invalid — which is the cache key working as designed.
