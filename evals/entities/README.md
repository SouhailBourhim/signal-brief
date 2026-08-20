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

## Refinements, added while labeling the first 300 (2026-08-20)

Written down as they were decided, and before `entities/resolve.py` exists, so they are
still protocol rather than post-hoc justification. Each one came up repeatedly in the real
corpus and the rule above did not settle it.

**`entity_id` namespace.** UPPERCASE is a tradable ticker (`META`, `CMCSA`, `PYPL`).
`lower-kebab-case` is an entity with no ticker — private, foreign-listed, or a fund
(`openai`, `unitree`, `chiba-bank`, `pier-88-investment-partners`). The case of the id is
therefore itself a claim about tradability, which is what SPEC §7.4's market-corroboration
component will need.

**A span links only if it contains the company's name.** Products and brands are not
companies, however unambiguously owned:

| Surface form | Answer | Why |
|---|---|---|
| `Meta AI` | `META` | contains the company name |
| `Google Drive` | `GOOGL` | contains the company name |
| `ChatGPT` | unlinked | a product; linking it to OpenAI is inference, not resolution |
| `AirPods`, `Windows`, `Chrome`, `Jira` | unlinked | same |
| `Venmo` | `PYPL` | a company name (a subsidiary), not merely a brand |
| `GitHub` | `MSFT` | a subsidiary whose parent is the tradable entity |
| `The Verge` | `the-verge` | a subsidiary whose parent (Vox Media) is *not* tradable, so it stays its own entity |

The line matters because an alias dictionary can easily be built that maps `ChatGPT` to
OpenAI, and it would then score well against labels that had been drawn the other way. The
labels are drawn first, so they constrain the dictionary rather than describing it.

**People are not companies.** EDGAR Form 4 and 144 filers are individuals (`King Alan`,
`GEE DAVID NICHOLAS`), and they are the single largest source of company-shaped spans in
this corpus.

**A span naming two entities at once is unlinked, `ambiguous`** — `Meta and Google` cannot
be one `entity_id`, and picking either would be wrong half the time.

**The Meta/metadata case turned up for real, as a river.** `Amazon` in *"oil discovery near
Amazon river"* is unlinked. This is the example the rule above was written against, and it
is in the set.

## What the first 300 look like

**54 linked · 246 unlinked** (244 `not-a-company`, 2 `ambiguous`) — 25 distinct tickers and
20 slugs.

An 82% abstention rate is not a defect in the sample. It is what a proper-noun heuristic
over real feeds actually yields, and it is the reason `score_entities` must count a correct
`unlinked` as a true negative: a resolver that links nothing would otherwise look perfect,
and one that links everything would too, depending on which half you forgot to count.
