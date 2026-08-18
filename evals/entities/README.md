# Entity resolution labeled mentions — SPEC §7.2

Empty until Phase 3. Target ~300 hand-labeled mentions.

## Labeling rule (written before labeling, per `../README.md`)

`entity_id` is the canonical company the surface form refers to **in this context**.

- "Meta" the company → `META`; "meta" as in metadata → **unlinked**.
- A subsidiary rolls up to its parent only when the parent is the tradable entity.
- A company that has renamed is labeled with the entity valid **at the article's
  publication date** — this is what `dim_entities` SCD2 exists for.
- Private companies with no ticker still get an `entity_id`; absence of a ticker is not
  absence of an entity.
- Below the confidence floor, the correct answer is **unlinked**, not a guess. The metric
  must reward abstention, or the resolver learns to guess.
