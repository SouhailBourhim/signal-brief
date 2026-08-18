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

## Current contents

55 pairs generated from the Phase 0 fixture, whose `story_key` field is ground truth by
construction. Real labeled pairs from live feeds replace these in Phase 3 — the fixture
proves the harness runs, not that the clustering is good.
