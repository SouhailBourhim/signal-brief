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
- [x] **252 pairs answered** (194 base-rate + 58 focus). 300 mentions still to answer.
- [x] `evals/label_dump.py` / `evals/label_apply.py` — dump candidates, apply answers,
      stamp provenance.
- [x] `evals/score.py` splits `dedup` from `dedup_fixture` and gains `--by-stratum`.

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

### Who labeled these, and what that costs the claim

**The 252 pair labels were made by an LLM assistant, not by the reader.** Every record
carries `"labeler": "claude-opus-5"`, and `evals/label_apply.py` requires the field rather
than defaulting it.

This is a real caveat, not a disclaimer. SPEC §12 frames labeling as the author's judgement,
and the brief exists to be useful to one specific reader; "dedup precision 0.339" therefore
means *agreement between the rule and a model*, not agreement with the person the brief is
for. Where the two would differ is exactly where it matters — a roundup, a follow-up that
adds one new fact, a press release beside the story written from it. The README states the
provenance beside the number.

The subset worth a human review pass is small and identifiable: the 39 false positives and
27 false negatives. Everything else is a pair both the rule and the labeler call the same way.

### The first draw had almost no positives, and that was structural

194 pairs came back with **one** positive. Not a sampling bug: the corpus is 63% SEC
filings, each a distinct company's distinct filing, and genuine syndication across three RSS
feeds inside 72 hours is rare. Recall over one example is not a measurement.

So `sample_pairs.py --focus` was added: it restricts to the non-filing articles — where two
sources *can* cover one event — and ranks pairs by IDF-weighted **title** token overlap. It
ranks on titles while `is_same_story` decides on title and body, so it is not merely
re-finding what the rule already merges; pairs are labeled blind, tagged `focus`, and scored
as their own stratum. 58 drawn, **46 positives**.

### Verified — the first honest dedup measurement

`uv run python evals/score.py --by-stratum`:

| stratum | n | positives | precision | recall |
|---|---|---|---|---|
| `near` | 58 | 0 | 0.000 | — |
| `borderline` | 76 | 1 | 0.000 | 0.000 |
| `random` | 60 | 0 | 0.000 | — |
| `focus` | 58 | 46 | **0.800** | **0.435** |
| combined | 252 | 47 | 0.339 | 0.426 |

**Read the strata, not the combined row.** `near`, `borderline` and `random` are base-rate
samples and describe what the brief's reader actually sees: 34 merges, every one of them
wrong, zero correct. `focus` is enriched for the positive class, so its 0.800 says how the
rule behaves once a plausible candidate is in front of it. Averaging the two describes
neither, which is why `stratum` is on every record and why `--by-stratum` exists.

Two independent failures, worth separating because 3.B has to fix both:

- **Precision.** Every merge on a representative sample is a false merge. 3.0 named the
  cause — EDGAR bodies are filing metadata, so SPEC §7.1 stage 1's boilerplate stripping,
  specified and never implemented, is doing no work.
- **Recall.** Even on `focus`, the rule misses **26 of 46** real same-story pairs. Those are
  cases like AP's *"NASA calls off Swift rescue mission"* against Ars's *"NASA calls off
  mission to rescue Swift gamma-ray observatory"* — one event, two outlets, few shared
  content words. This is the gap SPEC §7.1 stage 3 exists to close, and it is the evidence
  ADR-0009 will weigh when it rules on embeddings.

The base rate itself is a finding: **0 same-story pairs in 60 uniformly random ones.** A
`distinct_publisher_count` above 1 should be rare in this corpus, which makes the 3.0
brief's 22-publisher lead cluster even more clearly an artifact.

### What broke on first real use

**`silver.articles` holds 132 duplicate `article_id` rows across 2,849.** Found because the
sampler emitted articles paired with themselves.

`normalize_window` MERGEs on `article_id` with `WHEN NOT MATCHED THEN INSERT`, so this
should be impossible. Two shapes in the data:

    -- same article_id, byte-identical, same fetched_at to the microsecond
    020c4b73…  hackernews  fetched_at 2026-08-19 11:29:27.205641  x2

    -- same article_id, re-fetched 15 minutes later, same content_hash
    015a85e6…  edgar       fetched_at 11:44:42.122208 / 11:59:42.015826

The second shape is a source re-serving an entry across two hourly windows; the first is the
same bronze row normalized twice. Either way the MERGE did not match an existing row, and
the within-batch `dropDuplicates(["article_id"])` cannot see across runs. Not yet diagnosed
to a root cause — that belongs with 3.B, which is already rewriting this path.

Two consequences worth recording now:

1. **`exact_duplicates_removed` in the brief footer is partly counting this defect**, not
   syndication. Both shapes share a `content_hash`, so `exact_dedup` collapses them and the
   3.0 brief reported them among its 92 "exact dupes". A metric measuring a bug while
   appearing to measure the world.
2. The sampler **skips** self-pairs rather than deduplicating upstream, so the defect stays
   visible in `read_articles` and in `articles_in` instead of being papered over by the
   labeling tool.

## Then

3.B — dedup and clustering in Spark: the boilerplate stripping 3.0 found missing, a recall
fix for the 26 misses above, and the duplicate-`article_id` MERGE defect. 3.E ratchets the
floors and ADR-0009 rules on embeddings, against the numbers in this section.
