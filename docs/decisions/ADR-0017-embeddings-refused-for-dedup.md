# ADR-0017 — Embeddings are refused for same-story dedup, on the corpus measurement

**Status:** Accepted · **Date:** 2026-08-29 · **Reverses ADR-0009 §1 · ADR-0016 stands**

## Context

[ADR-0009](ADR-0009-embeddings-for-same-story-and-entities.md) §1 decided:

> **Embeddings are adopted for SPEC §7.1 stage 3, in Phase 4B, via Ollama — not
> sentence-transformers.** The measurement earns the stage; it does not dictate the vehicle.

The measurement behind that sentence was taken with `sentence-transformers`, which the same ADR
then rejected as the production vehicle on packaging grounds. [ADR-0016](ADR-0016-embedding-model-pin.md)
built the vehicle it chose instead — `nomic-embed-text` through Ollama, pinned by digest — and
5.C re-asked the question through the encoder that would actually ship.

**It does not survive the re-ask.** Not because the encoder is worse, but because the question
was measured on the wrong axis the first time.

## What was measured

Both against the deployed lake on 2026-08-29, through the shipping vehicle
(`evals/experiments/embed_dedup_ollama.py`).

### The labeled pairs say "maybe"

252 real pairs, 44 positive. The branch is a disjunction — `lexical OR cosine >= t` — because
that is how it would actually be added.

| rule | precision | recall | f1 |
|---|---|---|---|
| lexical only (ships today) | **0.962** | **0.568** | 0.714 |
| `>= 0.90` and above | 0.962 | 0.568 | 0.714 |
| `>= 0.85` | 0.912 | 0.705 | 0.795 |
| `>= 0.80` | 0.792 | 0.864 | 0.826 |

Every threshold from 0.90 up changes **nothing**: not one of the 19 positives the lexical rule
misses scores above 0.90. And the two classes overlap where it matters —

    the 19 missed positives:  min 0.7133 · median 0.8249 · max 0.8838
    the hardest true negatives: 0.8561, 0.8518, 0.8386, 0.8198, 0.8117 …

The median positive the rule misses sits *below* the third-highest negative. There is no clean
cut, only a trade.

### The corpus says "no"

This is the measurement 252 pairs structurally cannot make, and `evals/experiments/corpus_merge_rate.py`
exists because 3.B learned it the expensive way: pairwise precision 1.000 sat beside a single
cluster holding **59% of the corpus**. `group_stories` takes a transitive closure, so one false
edge merges two components permanently.

1,680 real heads from one window, all 1,410,360 pairs:

| threshold | false merges | rate |
|---|---|---|
| `>= 0.99` | 1,697 | 0.001203 |
| `>= 0.95` | 2,125 | 0.001507 |
| `>= 0.90` | 2,568 | 0.001821 |
| **`>= 0.85`** | **4,841** | **0.003432** |
| `>= 0.80` | 15,158 | 0.010748 |

**0.85 is the only threshold that buys any recall, and it produces 4,841 false edges over 1,680
nodes.** A spanning tree of 1,680 nodes needs 1,679. This is not a rate that degrades the
clustering; it is a rate that dissolves it — 3.B's mega-cluster, reproduced in advance and on
paper rather than in a morning's brief.

Even `>= 0.99` emits 1,697 edges, still above the spanning-tree threshold, while buying exactly
zero recall. There is no operating point.

**Why the merges are where they are.** They concentrate on templated titles — `424B2 - BANK OF
NOVA SCOTIA`, `424B2 - BANK OF MONTREAL /CAN/`, `D/A - <fund> a Series of CGF2021 LLC`. These
are genuinely near-identical strings describing genuinely unrelated filings, which is precisely
what an encoder trained for semantic retrieval is *supposed* to score as similar. The encoder is
not wrong. It is answering a question this corpus makes useless, and 64% of the corpus is that
question.

## Decision

**Do not add an embedding branch to `dedup.decide`.** The lexical rule ships unchanged, and
dedup's published figures stay 0.962 / 0.568 with a held-out recall of 0.500.

`evals/thresholds.toml`'s note that 0.500 is "the ceiling" stands, and its cause is now
narrower than "we have not tried embeddings yet": **the ceiling is a property of the alias index
and the corpus, not of the matching rule.** ADR-0009 said as much about the entity side —
"most of the remaining gap needs `?itemDescription` **and** a wider candidate set" — and the same
shape holds here.

## Consequences

- **ADR-0009 §1 is reversed and ADR-0009 §2 is confirmed.** That record rejected embeddings for
  entity resolution and adopted them for dedup; the adoption was the half that did not survive
  contact with the corpus. Its reasoning was not careless — it was measured — it was measured on
  the labeled set alone, which is the axis 3.B had already shown to be insufficient.
- **ADR-0016 stands entirely.** The embedding stage is built, pinned, cached and in production —
  for SPEC §7.4's novelty component, where the same encoder property that ruins dedup is exactly
  what is wanted: templated SEC filings *should* score as recycled narratives, and they do.
- The 274 MB model and the cache are not orphaned by this decision; they were paid for by
  novelty and dedup was going to be the second consumer.
- **A deferred item is now a refused one**, which is a better state. `evals/thresholds.toml` and
  `docs/runbooks/phase-5.md` carry the numbers rather than the intention.
- Re-entry, if anyone wants it: an encoder or a rule that separates *templated near-duplicate
  strings describing different filings* from *different phrasings of one story*. The EDGAR veto
  (3.E) does part of this lexically already; an embedding branch would have to sit behind it and
  would then only be scoring the editorial 30% of the corpus, which is where the labeled set's
  19 missed positives live. That is a real experiment and it is not this one.
