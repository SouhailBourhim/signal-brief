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
- [x] **252 pairs answered** (194 base-rate + 58 focus) and **300 mentions answered**.
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

**That review happened.** The 66 pairs where the rule and the labeler disagreed — 39 false
positives, 27 false negatives — were the whole reviewable surface; every other pair is one
they already agree on. The reader worked the seven genuinely contestable ones and **overrode
three**, all in the same cluster: a multi-product leak roundup (*"Apple Accidentally Leaked
More Than 10 New Products"*) is **not** the same story as coverage of one product from it,
and an op-ed responding to a leak is not the same story as the leak. Those records now carry
`"labeler": "souhail-bourhim"` and `"reviewed_from": "claude-opus-5"`, so the set says who
made the call that stands and who made the one it replaced.

The sensitivity was computed before the review and is worth keeping: flipping *every*
contestable label moved precision only 0.339 → 0.373. The three overrides that actually
landed changed recall 0.426 → 0.455 and left precision untouched. **Neither failure the
measurement identifies rests on a disputed judgement** — the 34 wrong merges on base-rate
strata and the ~23 missed real pairs survive any reading of the hard cases.

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
| `focus` | 58 | 43 | **0.800** | **0.465** |
| combined | 252 | 44 | 0.339 | 0.455 |

**Read the strata, not the combined row.** `near`, `borderline` and `random` are base-rate
samples and describe what the brief's reader actually sees: 34 merges, every one of them
wrong, zero correct. `focus` is enriched for the positive class, so its 0.800 says how the
rule behaves once a plausible candidate is in front of it. Averaging the two describes
neither, which is why `stratum` is on every record and why `--by-stratum` exists.

Two independent failures, worth separating because 3.B has to fix both:

- **Precision.** Every merge on a representative sample is a false merge. 3.0 named the
  cause — EDGAR bodies are filing metadata, so SPEC §7.1 stage 1's boilerplate stripping,
  specified and never implemented, is doing no work.
- **Recall.** Even on `focus`, the rule misses **23 of 43** real same-story pairs. Those are
  cases like AP's *"NASA calls off Swift rescue mission"* against Ars's *"NASA calls off
  mission to rescue Swift gamma-ray observatory"* — one event, two outlets, few shared
  content words. This is the gap SPEC §7.1 stage 3 exists to close, and it is the evidence
  ADR-0009 will weigh when it rules on embeddings.

The base rate itself is a finding: **0 same-story pairs in 60 uniformly random ones.** A
`distinct_publisher_count` above 1 should be rare in this corpus, which makes the 3.0
brief's 22-publisher lead cluster even more clearly an artifact.

### The mention labels, and the rules they forced

**300 answered: 54 linked, 246 unlinked** (244 `not-a-company`, 2 `ambiguous`), across 25
tickers and 20 slugs. `entities` now reports *"300 labeled, awaiting a decision function to
score against"* rather than the old "no labeled examples yet", which stopped being true.
Scoring waits on `entities/resolve.py` in 3.C, because the scorer must call the resolver
rather than reimplement it — the same contract `score_dedup` keeps with `is_same_story`.

An **82% abstention rate** is not a defect in the sample. It is what a proper-noun heuristic
over real feeds yields, and it is why `score_entities` has to count a correct `unlinked` as a
true negative: otherwise a resolver that links nothing looks perfect, and so does one that
links everything, depending which half you forgot to count.

Four rules were needed that the Phase 0 labeling rule did not settle, all recorded in
`evals/entities/README.md` as they were decided — before the resolver exists, so they remain
protocol rather than post-hoc justification:

1. **The id namespace carries a claim.** UPPERCASE is a tradable ticker, `lower-kebab` is an
   entity without one. SPEC §7.4's market-corroboration component will need exactly that
   distinction, and encoding it in the id means it cannot drift.
2. **A span links only if it contains the company's name.** `Meta AI` → `META`, but
   `ChatGPT`, `AirPods`, `Windows` and `Jira` are unlinked. This is the load-bearing one: an
   alias dictionary mapping `ChatGPT` to OpenAI is trivial to write and would score well
   against labels drawn the other way. Drawing the labels first is what stops the dictionary
   from grading itself. `Venmo` → `PYPL` and `GitHub` → `MSFT` because those are company
   names; `The Verge` stays `the-verge` because Vox Media is not tradable.
3. **People are not companies** — and EDGAR Form 4/144 filers are the largest source of
   company-shaped spans in this corpus.
4. **A span naming two entities is `ambiguous`**, not a coin flip.

**SPEC §7.2's own example turned up, as a river.** `Amazon` in *"oil discovery near Amazon
river"* is in the set, unlinked. The rule was written in Phase 0 against a hypothetical
Meta/metadata case; the real corpus supplied one within the first 300.

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

## 3.B — Dedup and clustering, part one: the decision *(done 2026-08-20)*

Split in two, as planned: the decision layer first — where `make eval` can measure the win
without a JVM — then the Spark job, blocking and tables. This is part one.

- [x] `dedup.strip_boilerplate` — SPEC §7.1 stage 1, specified in Phase 0 and never written.
- [x] Title and body compared separately, never pooled. The recall fix.
- [x] `prepare` / `decide` — tokens and simhash computed once per article, not once per pair;
      `decide` is the single shared decision and `is_same_story` wraps it for the eval.
- [x] Minimum-signal guards on all three branches: below them the comparison **abstains**
      rather than guessing, the same principle as the entity resolver's confidence floor.
- [x] `evals/fit_thresholds.py` — every constant fitted, on a train split, reported held out.
- [x] `MAX_CLUSTER_SHARE` / `MIN_CLUSTER_CAP` — the structural guard on transitive closure.
- [x] Floors raised off 0.0 in `thresholds.toml`.

### Verified

|  | precision | recall |
|---|---|---|
| train (fitted on) | 0.933 | 0.636 |
| **HELD OUT** | **1.000** | **0.500** |
| full set (what `make eval` gates) | 0.962 | 0.568 |
| Phase 0, same pairs | 0.000 | 0.455 |

The brief, same 72-hour window, before and after: **2,769 → 765 clusters with a 1,720-article
phantom at rank 1**, against **2,769 → 2,277 clusters** whose largest is a real 23-article,
8-publisher story (Disney suing the FCC) and whose lead is AP and Ars on the same NASA
decision. Clustering got faster too — 3.2 s to 1.3 s over the same 3.58M pairs — because
`prepare` runs once per article instead of tokenizing inside the loop.

### What broke on first real use

**Fixing precision broke the fixture, and the fixture was right.** The first cut scored
precision 1.000 on real pairs and dropped `dedup_fixture` recall to 0.714 — it had stopped
seeing *"Northwind acquires Lumen"* as *"Lumen to be bought by Northwind"*. The real corpus
is too thin in genuine rewrites to have caught that; the 55 synthetic pairs exist for exactly
this and are now a hard constraint in the fitter rather than only a gate.

**Constraining train precision to 1.000 overfits.** Cross-validation inside train showed a
0.90 constraint reaching the same CV precision with better CV recall — it stops the fit
chasing one split's particular negatives. Choosing that by looking at the held-out split
would have spent the split, so the constraint is selected by CV *inside train* under a stated
rule: never trade precision, but take recall that costs none.

**The pairwise eval cannot certify the clustering. This is the important one.**

After boilerplate stripping the pairwise numbers looked finished — precision 1.000 on every
base-rate stratum, zero false merges in 194 pairs. The brief still contained a single
1,575-article cluster holding 59% of the corpus.

Both statements are true, and the gap between them is structural. A 252-pair labeled set
cannot bound a false-merge rate that a clustering run applies to **3.58M pairs**, and
union-find takes a transitive closure, so one bad edge merges two components permanently. The
rate that was invisible in the eval, measured directly over random EDGAR pairs, was **1.9% on
the simhash branch and 1.4% on the title branch** — tens of thousands of false edges per
window, and a few well-placed ones chain everything.

Two fixes, and they are different kinds of thing:

1. **A guard the eval could have caught, had it been asked.** Stage 2 was the one branch
   given no minimum-signal guard — an oversight in this phase's own design. A simhash's
   discriminative power comes from having features to hash, and an EDGAR filing reduces to
   ~9 tokens, where collisions inside 12 bits are common. `MIN_SIMHASH_TOKENS = 25` took the
   cluster from 1,575 to 203. It is **held out of the fitting grid on purpose**: the pairwise
   objective scores every candidate identically, so fitting it would have produced a
   confident 0, and the fitter now says so in its output instead.
2. **A guard no pairwise metric can replace.** `MAX_CLUSTER_SHARE = 0.05` dissolves any
   component holding more than 5% of the window back into singletons, and reports the count
   — `ClusterResult.dissolved`, returned rather than logged, because a run that quietly
   dissolved 1,500 articles looks identical in the brief to one that never formed the
   cluster. Dissolving is the direction `evals/dedup/README.md`'s asymmetry points: a false
   split shows a visible, cheap duplicate; a false merge deletes a story nobody learns was
   missing. It is deterministic and order-independent, so a replay reproduces it exactly.

**The guard's floor had to be measured, not assumed.** `MIN_CLUSTER_CAP` started at 2 and
promptly dissolved the fixture's legitimate four-publisher event, because 5% of a ten-article
window is nothing. The largest genuine cluster in a real 72-hour window is 23 articles across
8 publishers; the false one beside it was 1,575. 50 sits in that gap.

**The labeled set could not distinguish `NEAR_DUPLICATE_DISTANCE` at all** — every value from
0 to 12 scores identically, because once boilerplate is stripped the title path already
catches what stage 2 would. So the tie is broken on SPEC §7.1's stated intent rather than on
noise, and the constant carries that reasoning instead of implying it was measured.

### Still open, carried into 3.B part two

- EDGAR still over-merges within one filer: a fund trust that lodged 47 supplements in a day
  becomes one 47-article cluster. Bounded and single-publisher, so `breadth` keeps it out of
  the brief, but it is not right.
- The 132 duplicate `article_id` rows are untouched. 3.B part two rewrites that MERGE path.
- `silver.articles.simhash` is now a **blocking** key only — the decision recomputes it over
  cleaned text. Part two's banded blocking is where that column starts earning its place
  again.

## Then

3.B part two — the Spark job, banded blocking, `silver.story_clusters` /
`silver.article_clusters`, and the duplicate-`article_id` defect. Then 3.C's resolver, which
`entities` is already labeled and waiting for.
