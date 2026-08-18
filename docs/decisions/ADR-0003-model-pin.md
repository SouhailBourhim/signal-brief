# ADR-0003 — Local model selection and pinning

**Status:** Proposed — measurement pending on the dev box · **Date:** 2026-08-18

## Context

SPEC §7.3 requires a pinned model digest, versioned prompts, temperature 0, and a
determinism boundary marked in lineage. It also assumes **~4 seconds per cluster**, which
was a CPU-era estimate written before the target hardware was known.

## Decision (pending measurement)

Run an 8B-class model at q4 (~4.7 GB, fits 8 GB VRAM with context headroom) via Ollama on
the host, pinned by digest in `signal_core.config.Settings.ollama_model_digest`, which
ships as `UNPINNED` and must be replaced before Phase 4.

## Required measurement

Before Phase 4, record here:

- model name and **full digest** (`ollama show --modelfile`)
- tokens/sec at temperature 0 on a representative cluster prompt
- wall-clock for 40 cluster summaries, the realistic pre-brief batch

Then **rewrite SPEC §7.3's capacity paragraph with the measured number.** The current
"~40 heads x ~4 s ≈ 3 minutes" is an assumption inherited from a CPU estimate, and SPEC
§15 says never publish a metric the pipeline cannot reproduce.

## Consequences

- Until the digest is pinned, the enrichment cache key is unstable and cache-hit rate is
  not a meaningful metric — which is why Phase 4 cannot start before this lands.
