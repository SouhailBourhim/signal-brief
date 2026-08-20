# Entity resolution labeled mentions — SPEC §7.2

Target ~300 hand-labeled mentions, gating Phase 3's acceptance (SPEC §12).

**Start now, not when the resolver is written.** `silver.articles` already holds real
articles to label against, and ~20 mentions a day through Phase 3's build reaches the
target by the time the code needs it. Labeling before the matching algorithm exists is
also the point of the rule below — it can't be tuned to flatter an implementation that
hasn't been written yet.

## Schema (`mentions.jsonl`, one JSON object per line)

```json
{"mention_id": "<article_id>:<char_start>", "article_id": "...",
 "published_at": "2026-08-18T14:03:22+00:00", "surface_form": "Meta",
 "context": "…±200 chars around the mention…",
 "entity_id": "META", "unlinked_reason": null, "origin": "silver-2026-08"}
```

| Field | Why it is here |
|---|---|
| `entity_id` | The answer. **Absent** means unanswered; **`null`** means deliberately unlinked, which is a correct answer and is scored as a true negative. |
| `published_at` | The rule below labels a renamed company with the entity valid **at publication date**. The scorer needs the date to query `dim_entities` as-of, or SCD2 is untestable. |
| `char_start` / `char_end` | Offsets into `title + "\n" + body_text` **as stored** — not into a cleaned copy. `silver.articles` is immutable; the cleaning rule is not, and an offset into text that gets re-cleaned in Phase 3.B would silently point somewhere else. |
| `context` | So a label can be made without re-querying the lake. |
| `unlinked_reason` | Unscored. `not-a-company` / `ambiguous` / `no-such-entity`. Records *why* a mention was left unlinked, so the resolver's failure modes can be read off the set rather than guessed at. |
| `origin` | `silver-<YYYY-MM>`. Keeps a draw traceable to the corpus it came from. |

Candidates come from `evals/sample_mentions.py`, which uses a **purely lexical**
proper-noun heuristic and never the entity dictionary. Sampling from dictionary hits would
publish recall-given-a-candidate — always higher than recall, and blind to every company
the dictionary has never heard of, which is exactly the private-companies-with-no-ticker
case the rule below calls out.

Expect some candidates to be obvious non-entities (`Filed`, `AccNo`, `Show HN`). They are
capped at four occurrences each and kept on purpose: a resolver that links them is wrong,
and a labeled set containing none of them cannot detect that.

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
