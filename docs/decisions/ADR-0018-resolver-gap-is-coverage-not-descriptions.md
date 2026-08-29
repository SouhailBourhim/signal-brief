# ADR-0018 — The resolver's gap is coverage and labels, not descriptions

**Status:** Accepted · **Date:** 2026-08-29 · **Completes ADR-0009 §2, and refuses the fix it named**

## Context

[ADR-0009](ADR-0009-embeddings-for-same-story-and-entities.md) §2 rejected embeddings for
SPEC §7.2 and reclassified the recall gap as a data problem rather than a modeling one. That
reclassification was right. The fix it named was not:

> The re-entry criterion is therefore a data change, and a cheap one: `entities/build.py`'s
> WDQS query already uses `SERVICE wikibase:label`, which serves `?itemDescription` from the
> same projection — one more variable in an existing `SELECT`. […] Note the ceiling is a
> property of the alias index, so most of the remaining gap needs `?itemDescription` **and** a
> wider candidate set — 20 of the 54 linked mentions name an entity the index never proposes.

The 20 are real and the ceiling of 0.630 is real. **But a description cannot address any of
them**, and that follows from what a description does: it arbitrates *between candidates*. A
mention that proposes no candidates has nothing to arbitrate.

Measured before the query was touched (`evals/experiments/candidate_ceiling.py`), because the
last time this project acted on ADR-0009 without re-measuring it cost a reversal (ADR-0017).

## What was measured

### The 20 unreachable mentions, split by cause

| cause | count | share of recall |
|---|---|---|
| **absent from the dictionary** | 17 | 0.315 |
| **present but not reachable through any alias** | 3 | 0.056 |

The 17 are private companies and EDGAR Form D filers (`Mishpacha Fund, LP`,
`JS VENTURE FUND LLC SERIES`, `Margaritaville Casey Key Investors, LLC`) and small technology
firms below Wikidata's notability floor (`Ollama`, `Unitree`, `EncroChat`, `Anthro Energy`).
The 3 are subsidiary rollups the alias index does not carry — `GitHub` → `MSFT`, `Venmo` →
`PYPL`.

**A description helps none of these**, and neither does a wider *crawl* for most of them:
a Form D filer is a private placement by a company with no ticker and no Wikipedia article.
Nine of the 17 state their CIK in the span's own context, which is a far stronger signal than
any description — and four of those already resolve correctly through minting, which the
alias-index ceiling does not model. **The 0.630 figure is a ceiling for context-scoring rules
over the alias index, not a ceiling on the resolver.**

### The 6 precision errors, one at a time

| span | resolved | labeled | what it actually is |
|---|---|---|---|
| `Cizzle Brands Corp` | `CZZLF` @1.00 | `cizzle-brands` | **a labeling error** |
| `Lyntris Inc.` | `LYNX` @1.00 | `lyntris` | **a labeling error** |
| `Section` | `investment` @0.75 | — | **a bug** — see below |
| `As AI` | `as-ai` @0.75 | — | **a bug** — same one |
| `USA Today Sparking` | `TDAY` | `GCI` | a parent-rollup judgement, left alone |
| `FlyWire` | `FLYW` @0.80 | — | the homonym case a description *could* arbitrate |

**Two were the eval being wrong, not the resolver.** Both spans state a CIK, and the resolver
linked through it at confidence 1.0 — the most certain channel there is. SEC's own
`cik-lookup-data.txt` confirms `0002105038` is CIZZLE BRANDS CORP and `0002132582` is
LYNTRIS INC., and both have tickered dictionary entries. The labels were minted slugs for
companies that already had canonical entities. Corrected, with `reviewed_from` recording the
label they replaced.

**Two were one bug.** `dictionary.has_legal_suffix` read
`any(token in LEGAL_SUFFIXES for token in tokens)` — *any position* — while its name, its
docstring and every caller said suffix. `LEGAL_SUFFIXES` contains `company` and `as`, which
are also an English noun and an English conjunction, so `Investment Company Act Section` minted
`investment` and `As AI becomes harder to avoid` minted `as-ai`.

**One is genuinely a description's job**, and it is one mention in three hundred:
`FlyWire` in "a 3D fruit fly … powered by the real FlyWire connectome" links to Flywire Corp,
the payments company. That is the `Apple`-the-fruit case ADR-0009 was reasoning about, and it
is exactly as rare in this corpus as the resolver's own docstring predicted — the corroborated
common-word channel "fires zero times across the 300 labeled mentions".

*(Recorded because it was nearly missed: the first pass of the diagnostic reported **zero**
arbitrable errors, because it only counted mentions whose labeled answer was among the
candidates. A description can also cause a **rejection**, and that is the entire `FlyWire`
case. The measurement was rerun.)*

## Decision

**`?itemDescription` is refused.** It addresses none of the 20 recall misses and at most one
of the six precision errors, against a ~30-minute WDQS rebuild, a larger committed dictionary,
and a new scoring channel to maintain. That is not a trade this evidence supports.

**What was done instead**, both falling directly out of the measurement:

1. The two labeling errors corrected against SEC's filer index.
2. `has_legal_suffix` made to mean what it says — the legal form must sit within
   `LEGAL_SUFFIX_MAX_FROM_END` of the end and have at least one token before it — plus a
   minted id must be more than one word.

| | precision | recall |
|---|---|---|
| before | 0.868 | 0.611 |
| + label corrections | 0.895 | 0.630 |
| + the suffix fix | **0.944** | **0.630** |
| **held out** (re-fitted) | **1.000** | **0.593** |

`CONFIDENCE_FLOOR` was re-fitted after the change — 3.E's lesson — and is unchanged at 0.72.

## Consequences

- **The uncommitted dictionary rebuild is now the better one, and is committed.** 5.C recorded
  it as a regression (0.842/0.593 against the committed 0.868/0.611) and left the decision
  open. The cause was the eval: `CZZLF` exists *only* in the rebuild, so the resolver was being
  penalised for correctly linking a company the newer dictionary knows about and the label had
  minted a slug for. With the label corrected the ordering reverses — 0.944 against 0.917.
- The `[entities]` floors ratchet to 0.90 / 0.60 for the first time since 3.C.
- **A ceiling that is a property of one channel should not be quoted as a property of the
  system.** ADR-0009's 0.630 was measured over the alias index alone, and four mentions it
  called unreachable resolve through minting. The figure was correct and its scope was not.
- Re-entry for descriptions: a corpus where the common-word class actually occurs. One mention
  in 300 is not it, and the resolver's own docstring said so before this was measured.
