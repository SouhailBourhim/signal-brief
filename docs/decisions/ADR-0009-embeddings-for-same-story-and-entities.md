# ADR-0009 — Embeddings: adopt for same-story in 4B, reject for entity resolution

**Status:** Accepted · **Date:** 2026-08-21

## Context

SPEC §7.1 names sentence-transformers as stage 3 of deduplication, and SPEC §7.2 names
"cosine similarity between article context and entity description embeddings" as the fix for
the class of mention a lexical resolver cannot settle. Phase 3 opened by deferring both, on
the record: the repo had no ML dependency, CI runs `uv sync --all-extras` on a CPU runner,
and both stages ship behind a single decision seam — `dedup.decide` and
`entities.resolve.resolve` — so the deferral was cheap to reverse. The runbook promised
this ADR "either way", and 3.B and 3.C both closed with a recall gap pointing at it:

- **Same-story: held-out recall 0.500.** The lexical rule finds half the genuine pairs.
- **Entities: held-out recall 0.556**, with `Meta` and `Apple` named as the largest single
  share of the misses — ordinary English words the resolver will not link on the word alone.

Two prior findings shaped how the question had to be asked. 3.B established that **a
252-pair labeled set cannot bound an error rate that clustering applies to millions of
pairs**: pairwise precision read 1.000 beside a single cluster holding 59% of the corpus,
because union-find takes a transitive closure and one bad edge merges two components
permanently. 3.D watched the same failure recur at a simhash threshold the labeled set
scored identically at every value. So "wins on the labeled pairs" is necessary and nowhere
near sufficient.

## What was measured

Three harnesses, committed under `evals/experiments/`, run in a throwaway virtualenv —
installing the dependency in order to decide whether to install it would have answered the
question by assumption.

Every choice that could flatter the challenger was taken from `evals/fit_thresholds.py`
rather than made fresh: the same seeded label-stratified halves, the same objective, the same
4-fold constraint selection *inside* train, the same hard fixture gate, and the same
minimum-signal guards and identifier veto out of `dedup.decide`. Model is
`all-MiniLM-L6-v2`; `all-mpnet-base-v2` was run as a control.

### Same-story — embeddings win, clearly

| | precision | recall |
|---|---|---|
| lexical, shipped constants | **1.000** | 0.500 |
| embedding, MiniLM | 0.870 | **0.909** |
| embedding, mpnet | 0.875 | **0.955** |

Held out, on pairs the fitting never saw. Recall roughly doubles. The two models agree, so
this is a property of embeddings rather than of one 90 MB model.

**The precision cost is real, bounded, and located.** All three held-out false merges fall in
the `focus` stratum — the draw deliberately enriched for plausible candidates — and are the
same failure: two different projects on one topic. Two Show HN private-AI-agent products; two
self-hosted search engines; two essays about recursive self-improvement. Across the
base-rate strata (`random`, `near`, `borderline`) the embedding rule makes **zero** false
merges, the same as lexical.

**The corpus-scale objection does not hold, and that is the finding that decides this.**
Over 200,000 random pairs from a real 4,633-article window, across three seeds:

| | false merges / 200,000 | projected edges per window |
|---|---|---|
| lexical | 0-1 | ~0-54 |
| embedding | 2-3 | ~107-161 |

Both are near zero, and roughly half the embedding's extra edges are **correct** merges the
random draw happened to catch (`LG Display Develops New OLED Panels` with `LG's new OLED
breakthrough`; the two Moderna/Merck vaccine stories). This is nowhere near the thousands of
edges 3.B measured for the old simhash branch, and nothing here chains.

That result depends on one thing, and it is worth stating separately because it was nearly
missed. **The embedding rule must inherit `decide`'s identifier veto.** Withheld, its
corpus-level false-merge rate is 0.347% against lexical's 0.025% — a 14× gap, entirely EDGAR
filings, because `424B2 - Morgan Stanley Finance LLC` and `424B2 - Nomura America Finance,
LLC` genuinely *are* the same kind of text at cosine 0.79. An encoder cannot tell two
prospectuses apart; the accession number can, and the lexical rule was already reading it.
The guard is structural, not lexical, and it belongs to the pipeline rather than to any one
stage.

### Entities — embeddings lose, and not because of the encoder

| | precision | recall |
|---|---|---|
| lexical, shipped | 0.833 | 0.556 |
| embedding alone | 1.000 | **0.000** |
| lexical + embedding | 0.833 | 0.556 |

Held out. The hybrid is identical to lexical at every threshold clearing the precision
constraint: the embedding stage contributes **nothing at all**.

The reason is upstream of the model. `warehouse/entities/dictionary.json.gz` holds a
canonical name, a ticker, a CIK, a rank and aliases — **no descriptions**. SPEC §7.2 says
"entity description embeddings" and there are none to embed, so the harness must synthesise
one from the available fields (`Apple Inc., traded as AAPL, a public company`). That string
says nothing about what the company *does*, which is precisely the signal that separates
`Apple` the company from `apple` the fruit. Cosine against a headline is noise.

And the ceiling sits below the shipped number regardless of encoder quality: the alias index
proposes the correct entity for **34 of 54** linked mentions, so **recall for any
context-scoring rule over this dictionary is capped at 0.630** — against 0.611 shipped. Even
a perfect scorer buys 0.019, and only by being perfect.

## Decision

**1. Embeddings are adopted for SPEC §7.1 stage 3, in Phase 4B, via Ollama — not
sentence-transformers.** The measurement earns the stage; it does not dictate the vehicle.

`sentence-transformers` costs **1.1 GB installed** (722 MB of it torch) on every
`uv sync --all-extras`, in a repo whose bronze-path architecture exists because of a 250 MB
packaging ceiling. Ollama costs nothing new: ADR-0002 already places it natively on the host,
4B already depends on it, and reaching it is an httpx call to a dependency the Lambda extra
already ships. The stage also needs a content-hash cache to keep replay deterministic —
SPEC §12's 4B acceptance already requires exactly that machinery, keyed on
`hashing.enrichment_cache_key`.

So the stage lands where its cache, its cost accounting and its model pin already are, one
phase later, rather than dragging a second inference stack into Phase 3 to arrive sooner.

**2. Embeddings are rejected for SPEC §7.2, and the recall gap is reclassified.** It was
recorded through 3.C as blocked on an ML dependency. It is not. It is blocked on **a data
asset the dictionary does not have**, and buying torch would not have moved it. The
re-entry criterion is therefore a data change, and a cheap one: `entities/build.py`'s WDQS
query already uses `SERVICE wikibase:label`, which serves `?itemDescription` from the same
projection — one more variable in an existing `SELECT`. Checked against the live endpoint
rather than assumed, and the two entities that name the problem answer it directly:

    Q312  Apple Inc.       "American multinational technology company based in Cupertino, California"
    Q380  Meta Platforms   "American technology company"

That is the signal a synthesised description does not carry, and it is the one that
distinguishes `Apple` the company from the fruit. Descriptions first, measured against the
0.630 ceiling; embeddings only if that ceiling moves. Note the ceiling is a property of the
alias index, so most of the remaining gap needs `?itemDescription` **and** a wider candidate
set — 20 of the 54 linked mentions name an entity the index never proposes at all.

**3. `NEAR_DUPLICATE_DISTANCE` leaves the fitting grid.** Not an embedding question, but
found while asking one: the grid was recommending 12 while `dedup.py` shipped 0, because the
labeled set scores every value identically and a tuple-order tiebreak was choosing. 12 is the
value 3.D removed for chaining a 45-article false cluster. It joins `MIN_SIMHASH_TOKENS` as a
constant set from `evals/experiments/corpus_merge_rate.py` instead, and the fitter now breaks
ties by keeping what ships.

## Consequences

**The published dedup recall stays 0.500 through 4A, and the reason is now on the record
rather than open.** Anyone reading `evals/thresholds.toml` can see what the ceiling is, what
lifts it, and when.

**`dedup.decide` stays the single seam.** The 4B change is one branch inside it plus a cache,
because 3.B put the decision in one function and the eval scores that function rather than a
copy. This ADR is the payoff for that.

**The identifier veto is now load-bearing for a stage that does not exist yet.** It is
pinned by `tests/test_transform_dedup.py::test_two_filings_by_one_company_are_two_stories`,
and 4B must not implement the embedding branch ahead of it.

**Three harnesses are committed and none of them runs in CI.** `embed_dedup.py`,
`embed_entities.py` and `corpus_merge_rate.py` require a dependency the repo deliberately
does not have; only the last runs on the repo's own environment. They are here so a future
phase re-opens this with a measurement rather than an argument, and each carries the command
that reproduces it.

**`corpus_merge_rate.py` outlives this decision.** 3.B and 3.C both improvised the
corpus-level false-merge measurement and 3.D needed it a third time. It is the only thing
that can decide a constant the labeled set scores flat — which, measured in 3.E, is three of
the four the grid searches.

**What would reverse this.** A corpus with real syndication. `dedup_ratio` here is 1.01, so
this corpus barely contains duplicates to find, and the whole comparison rests on 44 positive
pairs. If a newswire source lands and the ratio moves, both the recall gap and the false-merge
budget change shape, and the decision to wait for 4B is worth re-asking against that corpus.
