# ADR-0001 — No Kafka in Phases 1-4

**Status:** Accepted · **Date:** 2026-08-18

## Context

The v2 design (`docs/archive/signal-design.md`) put Kafka at the centre of the
architecture: Spark normalize produced to `articles.normalized`, and Spark Structured
Streaming consumed it for clustering and entity resolution. The narrowed plan
(`docs/archive/PROJECT_START.md`) kept the topic but deferred Structured Streaming — which
left a topic whose only consumer had been postponed.

Kafka was reinstated briefly during planning, then withdrawn.

## Decision

No Kafka and no Structured Streaming through Phase 4. Batch PySpark on the 15-minute
cadence instead. Re-entry criteria are recorded in SPEC §14 and are deliberately strict:
a source that is **genuinely continuous rather than polled**, *and* a second independent
consumer of the topic that is not the batch clustering job.

## Rationale

Two arguments, in order of weight.

1. **The sources are micro-batch by nature.** Every feed is reached by polling on a 5-30
   minute cadence. There is no continuous upstream stream to preserve, so a topic in the
   middle would be manufacturing streaming semantics the data never had — and then
   defending exactly-once delivery for a pipeline whose inputs arrive in files.
2. **It had one consumer and no process boundary.** The topic sat between one local Spark
   job and another, while `bronze/` was already the immutable replay source of truth. A
   topic in that position is a résumé line that has to be defended rather than a design
   that carries weight.

## Consequences

- Replay comes from `bronze/`, which is stronger: it survives topic retention entirely.
- "Streaming" leaves the résumé until it is true. The honest version — "designed a
  streaming path, measured what it bought, removed it" — is a better interview answer than
  a topic nobody consumes.
- If a genuinely continuous source is added later, this ADR is superseded rather than
  quietly ignored.
