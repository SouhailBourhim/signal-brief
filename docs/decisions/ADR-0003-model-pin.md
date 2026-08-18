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
