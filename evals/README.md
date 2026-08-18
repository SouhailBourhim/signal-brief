# Evaluation sets

SPEC §11: an accuracy regression fails the build. This directory is the machinery that
makes that true, and it exists before the accuracy does so that Phase 3 cannot quietly
skip it.

```bash
make eval          # score everything
python evals/score.py --gate --only dedup
```

## Protocol first, labels second

Write the labeling rule down **before** labeling, or 200 labels drift as the labeler's
definition moves. Each subdirectory states its rule.

| Set | Question | Size | Phase |
|---|---|---|---|
| `dedup/` | Do these two articles describe the same event? | 55 (fixture) → ~200 real | 0 → 3 |
| `entities/` | Does this mention resolve to this entity? | ~300 | 3 |
| `enrichment/` | Is this LLM output correct for this cluster? | ~100 | 4 |

## Why the scorers import the pipeline

`score.py` calls `signal_core.dedup.is_same_story` — the same function clustering uses. An
eval that reimplements the rule measures a system nobody ships, which is the usual reason
published accuracy numbers do not survive contact with an interviewer.
