# Phase 3 runbook — cluster + resolve

Exit condition (SPEC §12): reported precision/recall on both labeled sets, reproducible via
`make eval` — **and a real brief has been read every morning since 3.0**, not a fake one.

Carried forward from Phase 2: nothing blocking. Phase 1's 1.D was still mid-outage when
this phase started (T0 = 2026-08-20T12:37:25Z), which is why the first real briefs report a
`degraded` footer — that is the monitoring layer working, not a Phase 3 defect.

## Decisions taken before starting

- **No embeddings yet.** SPEC §7.1 stage 3 names sentence-transformers, but the repo has no
  ML dependency and CI runs `uv sync --all-extras` on a CPU runner. Clustering ships behind
  the existing `dedup.is_same_story` seam; ~200 real pairs get labeled; the measurement
  decides. ADR-0009 will record the verdict either way. Ollama embeddings with a vector
  cache keyed on the existing `hashing.enrichment_cache_key` is the named upgrade path,
  cheap to reach because `is_same_story` is already the single decision point.
- **Spark transforms, Athena reads.** The cluster job is a Spark transform shaped like
  `normalize_window`. The brief is a *query*, so it goes through `ops/athena.py::run_query`.
  That keeps SPEC §10.1's egress off the dev box — Athena scans inside AWS and returns only
  result rows — and it is what finally populates `RunHealth.bytes_scanned` and
  `.estimated_cost_usd`, declared since Phase 0 and never filled by anything.
- **Thresholds are not fitted before labels exist.** The code can be written first; the
  constants cannot. This is SPEC §12's "label before writing the matcher", and it is what
  stops the published precision/recall from describing a rule bent to agree with it.

## 3.0 — A real brief *(done 2026-08-20)*

- [x] `brief/read.py` — `read_articles` and `read_health` over Athena, plus the coercion
      boundary. Everything Athena returns is a string or None, so this module is mostly
      about turning that back into typed rows.
- [x] `brief/build.py` — the four-stage run, the analogue of `skeleton.run`: read → health →
      cluster → rank + render. Phase 0's `exact_dedup` + `group_stories` unchanged, per
      ADR-0008 §2's "3.0 is a wiring job".
- [x] `signal brief` subcommand; `make brief` builds then opens, `make brief-open` just
      opens. The old target only opened, and did so with macOS `open` on a WSL2-only
      project (ADR-0002) — it printed a path and opened nothing on the machine the brief is
      actually read on. `wslview` first now, then `xdg-open`, then `open`.
- [x] `RunHealth.to_dict` emits `bytes_scanned`, and the footer renders it beside the cost.
      At this lake's size every query floors at Athena's 10 MB minimum and rounds to the
      same dollar figure, so bytes scanned is the number that actually moves — reporting
      only the cost would look like nothing ever changes (`docs/athena.md` says the same
      about the README's figures).
- [x] `unmonitored` — a source with no verdict in `ops.source_health` at all is reported,
      not omitted. It is the one status `assess_source` cannot return, and it is in
      `DEGRADED_STATUSES` because a source monitoring has lost track of must not render as
      a clean footer. Same bug as `thin` in 1.E, one layer out.

### Verified — the first brief built from real data

`uv run signal brief --limit 10`, against real AWS, 2026-08-20T15:54Z:

| | |
|---|---|
| Window | 72h (SPEC §7.1's same-story window) |
| Articles read | **2,769** |
| Bytes scanned | **490,829** (two queries; well under the workgroup's 100 MB cutoff) |
| Cost | **$0.000048** — the 10 MB floor, not the actual scan |
| Exact duplicates removed | 92 |
| Clusters out | **765** |
| Pairs compared | **3,581,826 in 3.2 s** |
| Wall clock, end to end | 10.7 s |
| Footer | `degraded` — all six sources, 1.D's outage |

Source mix: `edgar` 1,547 · `hackernews` 920 · `edgar_formd` 201 · `rss_tech` 47 ·
`rss_verge` 35 · `rss_ars` 19. By publisher, `sec.gov` is 1,749 of 2,769 — **63% of the
corpus is SEC filings**, which ADR-0008 predicted would make the first brief a bad one.

The O(n²) cost is real but not yet the binding constraint: 3.6M pairs in 3.2 s. `dedup.py`
predicted this would need the banded-LSH rewrite; at this volume it does not, and 3.B's
blocking is justified by correctness and by growth, not by today's clock. Recorded so the
claim is measured rather than assumed.

### What broke on first real use

**Athena renders Iceberg timestamps as `2026-08-19 12:22:49.000000 UTC`.** Not `.000`, and
with a trailing zone name. `_parse_timestamp` was written against the guess and raised on
the first real row. Raising was the right call and is kept: returning `None` would have
rendered a format change as a null column, and a null `published_at` is *meaningful* — it
makes the ranker distrust the article and fall back to `fetched_at` (SPEC §6.2). A brief in
which nothing had ever been published would have looked plausible and been wrong.

**The fix for that introduced a second bug in the same function.** Normalizing the ISO
`T` separator with `.replace("T", " ")` also eats the T out of the word `UTC`, leaving
`...49.000000 U C` and an unmatchable zone. Strip the zone suffix *first*, then normalize
the separator, and match only a `T` between digits. The offset form is converted rather
than trimmed — discarding a `+02:00` would shift every timestamp in the brief by two hours,
which is exactly what `timeutil.ensure_utc` exists to refuse to do. All five shapes Athena
and Trino emit are now pinned in `tests/test_brief_read.py`.

**`group_stories` merged 1,720 of 2,677 articles into a single cluster.** 64% of the
corpus, presented as one story, ranked first by breadth with 22 "independent publishers".
This is the false-merge failure `evals/dedup/README.md` says precision 1.00 exists to
prevent: *"A false merge deletes a story from the brief, which the reader never sees and
therefore cannot report."*

Cluster-size histogram: 708 singletons, 45 pairs, 6 triples, then 4, 7, 13, 27, 90, **1720**.

It is not a chain artifact. Over 4,000 **randomly sampled** EDGAR pairs — no blocking, no
transitivity:

| | |
|---|---|
| Jaccard p50 / p90 / p99 | **0.318** / 0.429 / 0.526 |
| `SAME_STORY_JACCARD` | 0.25 |
| Pairs at or above threshold | **82.1%** |
| Pairs within `NEAR_DUPLICATE_DISTANCE` (14) | 2.2% |
| `is_same_story` returns True | **82.1%** |

The median pair of *unrelated* SEC filings scores above the same-story threshold. Simhash is
not the culprit; the lexical Jaccard rule is doing all of the damage.

The cause is visible in one token set. A whole EDGAR article tokenizes to:

    ['0001654954', '0001900304', '007739', '08', '19', '2026', '26', '271',
     'accno', 'filed', 'filer', 'haleon', 'kb', 'plc']

Twelve of fourteen tokens are filing boilerplate (`filed`, `accno`, `kb`, `filer`), the
shared filing date (`2026`, `08`, `19`, `26`), or accession-number digits. Only `haleon`
and `plc` are topical. Two filings lodged on the same day therefore share most of their
vocabulary by construction.

**`parse/edgar.py` is not at fault, and neither is the threshold.** The parser faithfully
stores EDGAR's `<summary>`, and that summary genuinely contains nothing but filing
metadata — EDGAR's Atom feed carries no prose body. This is a true fact about the source.
What is missing is SPEC §7.1 stage 1's *"content hash after **boilerplate stripping**"*,
which was specified and never implemented. No threshold repairs a token set that is 85%
boilerplate, and neither would embeddings: the text genuinely does not describe an event.

Two consequences for 3.B, both recorded before any label exists so they cannot be
retrofitted to flatter a measurement:

1. Boilerplate stripping is a prerequisite for clustering, not a refinement of it.
2. Union-find over a transitive closure has no cluster-size sanity bound. One bad edge is
   permanent and unbounded, and a cluster holding 64% of the corpus should have been an
   error, not a headline.

## 3.A — Labeling starts *(sampled 2026-08-20; labeling ongoing)*

- [x] `evals/sample_pairs.py` — stratified candidate pairs into `evals/dedup/candidates.jsonl`.
- [x] `evals/sample_mentions.py` — candidate mentions into `evals/entities/mentions.jsonl`.
- [x] `evals/entities/README.md` gains the record schema. The labeling *rule* was already
      written; the schema was not, and a rule without a schema cannot be labeled against.
- [x] `evals/dedup/README.md` documents the candidates/pairs split and the two origins.
- [ ] ~200 pairs and ~300 mentions answered, at roughly 20/day alongside the build.

Neither sampler pre-fills an answer. `hamming` and `jaccard` are used to *stratify* — to
find pairs where the decision is hard — and never to guess; `sample_mentions.py` does not
consult the entity dictionary at all. Both properties exist for the same reason SPEC §12
puts labeling before the matcher: a label set shaped by the rule it judges measures
nothing.

### The sampler had to be built around 3.0's finding

`sec.gov` is 63% of the corpus and 82% of random EDGAR pairs clear `SAME_STORY_JACCARD`. A
uniform draw would therefore have been mostly one degenerate case, and a published number
computed over it would describe SEC filings rather than this pipeline. So pairs are
stratified twice — by similarity band (`near` / `borderline` / `random`) and by which kinds
of source they join — with a 40% cap on any one class.

Filings are still represented, deliberately. That is where the rule fails, and a label set
that excluded them would report a precision the brief does not have.

Measured on the first draw: **194 candidates, `sec.gov` in 88 of them (45%)** — down from
63% of the corpus, without excluding it. Stratum split `borderline=76 near=58 random=60`.

The third, uniform-random stratum is not redundant. `near` and `borderline` sit on the
current rule's decision boundary by construction, and recall measured only near a boundary
is a flattering number.

### Verified — the strata produce pairs worth a human's time

A `near` pair the rule would merge and a human would not:

    A [sec.gov]  4 - TRINET GROUP, INC. (0000937098) (Issuer)
    B [cato.org] Who Will Pay for Democratic Socialism's $200T Cost?

A `borderline` pair that is a genuine judgement call:

    A [kernelkennel.com] Packing Malware in Rosetta 2
    B [github.com]       Rust arrayref 0.3.10 crate installs malware

300 candidate mentions drawn across 261 distinct surface forms, capped at 25% from filings
— an EDGAR title carries its company name *and* its CIK, so those mentions are structurally
free to resolve, and letting them fill the sample would publish an accuracy earned on the
easy half of the corpus.

Roughly a tenth of the mention candidates are filing boilerplate (`Filed`, `AccNo`,
`Filer`, `Show HN`), capped at four occurrences each. These are kept rather than filtered:
a resolver that links them is wrong, and a labeled set containing none of them cannot
detect that.

Mention offsets are into `title + "\n" + body_text` **as stored**, not into a cleaned copy.
`silver.articles` is immutable and the cleaning rule is not — 3.B is about to change it —
so an offset into cleaned text would silently point somewhere else afterwards.

## Then

3.B — dedup and clustering in Spark, and the boilerplate stripping 3.0 found missing.
Thresholds stay untouched until the labels above exist.
