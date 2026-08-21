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
| `dedup/` | Do these two articles describe the same event? | 55 fixture + 252 real | 0 → 3 |
| `entities/` | Does this mention resolve to this entity? | 300 | 3 |
| `enrichment/` | Is this LLM output correct for this cluster? | ~100 | 4 |

## Why the scorers import the pipeline

`score.py` calls `signal_core.dedup.is_same_story` — the same function clustering uses. An
eval that reimplements the rule measures a system nobody ships, which is the usual reason
published accuracy numbers do not survive contact with an interviewer.

## `experiments/` is not part of the gate, on purpose

`score.py` and `fit_thresholds.py` run on the repo's own environment and CI enforces them.
`experiments/` holds measurements that answer a question the gate cannot:

| Script | Question | Needs |
|---|---|---|
| `embed_dedup.py` | Do sentence embeddings beat the lexical same-story rule? | a throwaway venv with `sentence-transformers` |
| `embed_entities.py` | Do they beat the lexical resolver on the `Meta`/`Apple` class? | the same |
| `embed_corpus.py` | What does each rule merge over random *real* pairs? | Athena for the dump; the venv for scoring |
| `corpus_merge_rate.py` | Per-branch false-merge rate, and what a constant costs | Athena for the dump only |

Two rules keep these honest. **They import the split, objective and constraint selection
from `fit_thresholds.py`** rather than choosing their own — a challenger allowed a friendlier
bar than the incumbent proves nothing. And **the dependency stays out of `pyproject.toml`**:
adding 1.1 GB in order to decide whether 1.1 GB is worth adding answers the question by
assumption. ADR-0009 is the verdict; each script's docstring carries the command.

`corpus_merge_rate.py` is the one that outlives its ADR. A 252-pair labeled set cannot bound
an error rate that clustering applies to ten million pairs, and it determines only one of the
four constants in the same-story grid. The rest are decided here or by a tiebreak.
