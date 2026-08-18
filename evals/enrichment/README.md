# LLM enrichment labeled examples — SPEC §7.3

Empty until Phase 4. Target 100 labeled examples, scored per model digest and prompt
version so swapping a model is a measurement rather than a vibe.

## Labeling rule

For each cluster, record the correct one-sentence summary, topic, and structured
extraction. Score:

- **summary** — factually entailed by the source articles, no invented figures. A fluent
  summary containing a number that appears nowhere in the source is a failure, not a near
  miss.
- **topic** — exact match against the accepted-values list.
- **extraction** — field-level exact match, with `null` a valid and often correct answer.

Schema-invalid output is not scored here; it is counted separately as the schema-failure
rate and quarantined to `gold.enrichment_rejects`.
