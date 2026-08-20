# Dedup labeled pairs — SPEC §7.1

**Schema** (`pairs.jsonl`, one JSON object per line):

```json
{"pair_id": "...", "a": {"title": "...", "body": "...", "publisher": "..."},
 "b": {"title": "...", "body": "...", "publisher": "..."},
 "same_story": true, "origin": "phase0-fixture"}
```

## Labeling rule

`same_story` is true when both articles report **the same underlying event**, regardless of
wording, angle, or which detail leads.

- Same acquisition, same filing, same release, same print → **true**, even if one is a
  200-word wire item and the other a 2,000-word analysis.
- A follow-up that adds genuinely new facts (a regulator responds; the deal collapses) →
  **false**. It is a new event in the same narrative.
- A roundup covering five unrelated events → **false** against all of them. Roundups are
  a known weakness; label them and let the metric show the cost.
- Correction notices and reprints of the same copy → **true**.

Ties break toward **false**. A false merge deletes a story from the brief, which the
reader never sees and therefore cannot report; a false split shows a duplicate, which is
visible and cheap. That asymmetry is why `thresholds.toml` demands precision 1.00 and
tolerates lower recall.

## How a pair gets here

`evals/sample_pairs.py` draws candidates from real `silver.articles` into
`candidates.jsonl`, **without a `same_story` key**. Labeling is moving a line into
`pairs.jsonl` with the answer added, so the scorer never reasons about a null and the lines
left in `candidates.jsonl` are a progress bar.

Candidates carry an extra `stratum` field (`near` / `borderline` / `random`). It is not
scored. It exists so Phase 3.E can report *where* the rule fails rather than only that it
does — a precision figure that is fine on news and catastrophic on filings is two facts,
and their average is neither.

The sampler stratifies by similarity band **and** by what kinds of source a pair joins,
capping any one class. `sec.gov` is 63% of the corpus and its pairs are degenerate — 82% of
random EDGAR pairs clear `SAME_STORY_JACCARD`, because an EDGAR body is filing metadata
rather than prose (`docs/runbooks/phase-3.md`, 3.0). An unstratified sample would therefore
measure filings instead of the pipeline. Filings stay represented, because that is where
the rule fails and hiding it would be the dishonest move; they just do not crowd out the
syndication cases §7.1 exists to catch.

## Current contents

Two origins coexist, and they are scored separately:

| `origin` | n | What it is |
|---|---|---|
| `phase0-fixture` | 55 | Generated from the Phase 0 fixture, whose `story_key` is ground truth by construction. Kept as a harness canary — it proves the scorer still runs, not that the clustering is good. |
| `silver-<YYYY-MM>` | → ~200 | Real pairs from live feeds. **This is the set the published number comes from.** |

They are gated separately (`[dedup]` and `[dedup_fixture]` in `../thresholds.toml`) because
the fixture pairs are trivially correct: folding them into one score would let 55 free
passes mask roughly a fifth of the real set's failure headroom.
