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

## 3.B — part two: the Spark job *(done 2026-08-20)*

- [x] `spark/jobs/cluster.py` — `cluster_window`, the two tables, and their DDL.
- [x] `dedup.blocking_keys` — prefix filtering, and `dedup.group_edges`, so the Spark path
      and the in-process path share every step after the pairwise decision.
- [x] `airflow/dags/cluster_dag.py` on a daily cron; `SILVER_COMMITTED` /
      `CLUSTERS_COMMITTED` assets; `process` now emits the first of them.
- [x] Serializable merge isolation on `silver`'s tables, for the duplicate-`article_id`
      defect below.

### Blocking is prefix filtering, not LSH — because it can be exact here

The plan called for banded LSH. Banding a 64-bit simhash at Hamming ≤ 12 needs ≥ 13 bands,
so ≤ 4-bit bands, so 16 buckets per band — which at 2,700 articles proposes *more* pairs than
all-pairs does. LSH does not work at this threshold.

It also solves the wrong problem. The decision is now a **token-overlap threshold**, not a
distance, and for that there is an exact method: sort a token set by descending global
frequency and index the article under its first `|A| - ceil(t*|A|) + 1` tokens. Any two sets
with Jaccard ≥ t are then guaranteed to share an indexed token, so blocking loses no
candidate — a strictly stronger guarantee than LSH offers. Rarest-first is what also makes it
cheap: a bucket keyed on `ai` is most of the corpus, one keyed on `unitree` is two articles.
Simhash banding survives only for stage 2, where approximate is all a blocking key has to be.

**Measured on a real 72-hour window: 447,427 candidate pairs against all-pairs' 3,581,826 —
12.5%, an 8x reduction, with 10,840 surviving edges.**

### Verified — the job against real AWS

| | |
|---|---|
| articles in / clusters out | 2,769 → **2,284** |
| candidate pairs | 447,427 (12.5% of all-pairs) |
| edges after the decision | 10,840 |
| oversized clusters dissolved | 1 (203 articles) |
| blocking keys dropped | 3 |
| `ordering_key` | `fetched_at,article_id@6a2bdbb9dcaa4c61` |
| runtime | 27 s, including JVM start and Iceberg jar resolution |

`silver.story_clusters` and `silver.article_clusters` are live in Glue and queryable:
2,284 clusters, largest 47 articles, **18 of them covered by more than one publisher** —
which is the number the brief's `breadth` component actually has to work with.

### What broke on first real use

**Blocking is exact; the bucket cap is not, and the difference shows up only at scale.**
The Spark job returns 2,284 clusters where all-pairs returns 2,277. Prefix filtering did not
lose those seven: `MAX_BLOCK_SIZE` did. Three keys held more than 400 articles and were
dropped rather than exploded into 320k pairs each, and dropping a bucket drops the candidate
pairs inside it.

Seven clusters in 2,284 is the measured price, `blocking_keys_dropped` reports it rather
than swallowing it, and `test_blocking_finds_every_pair_all_pairs_would` now **asserts the
precondition** — `blocking_keys_dropped == 0` — instead of claiming an exactness the cap can
void. A test that silently depended on a fixture too small to trip the cap would have gone
green forever while the property it named stopped being true.

**The duplicate `article_id` rows are one incident, and the mechanism is worth knowing.**
All 132 ids are confined to articles fetched inside a single hour — 2026-08-19 11:00–12:00
UTC — which is 2.E's session, where `process` was first unpaused and manually triggered
alongside its own schedule.

`MERGE ... WHEN NOT MATCHED THEN INSERT` is **not a uniqueness constraint**. It compiles to
an append, and Iceberg appends never conflict with one another, so two writers that both read
a pre-insert snapshot both find NOT MATCHED and both insert. `max_active_runs=1` stops two
DAG *runs*; it does not stop a manual trigger racing a scheduled one.

`ensure_tables` now sets `write.merge.isolation-level = serializable`, by `ALTER` as well as
`CREATE` — `CREATE TABLE IF NOT EXISTS` is a no-op against a live table, the same trap
`health_snapshot` hit with added columns in 1.E. A conflicting second writer now fails loudly
instead of duplicating quietly. `test_article_id_stays_unique_across_reruns` pins the
contract, while being honest that it is sequential and cannot reproduce the concurrent case.

**The 132 existing duplicate rows are still in the table.** Removing them is a destructive
write to the lake and it needs a decision, not a commit: they are collapsed by
`exact_dedup` before clustering, so nothing downstream is wrong today — but
`exact_duplicates_removed` in the brief footer is counting them as if they were syndication.

## 3.B.2 — A filing is not a story *(done 2026-08-20)*

The largest cluster in the table was 47 filings by one fund trust, lodged on one day. Their
titles are **byte-identical** — `497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)`, title
overlap 1.000 — and the only thing that distinguishes them is the accession number, which
3.B had thrown away as a "long digit identifier".

That was the right instinct applied one step too far. A CIK adds nothing to a *topical*
overlap, so it belongs out of the token sets. But an accession number is the document's
**identity**, and identity is exactly what tells two documents apart when everything else
about them agrees.

So `prepare` keeps identifiers alongside the token sets, and `decide` opens with a veto:
**two documents that each carry identifiers and carry different ones are different
documents**, however completely the rest of them agrees. It fires only when both sides have
them, so ordinary prose — which has none — is untouched, and one-sided evidence is not read
as disagreement.

### Verified

The labeled set is **unchanged**: precision 0.962, recall 0.568, fixture 1.000/1.000, and
the fit still selects the same thresholds. That is the expected result and it is the reason
to check — news prose carries no long identifiers, so a rule aimed at filings should cost
nothing on news, and now it is measured rather than assumed.

The corpus is transformed:

| | before | after |
|---|---|---|
| edges surviving the decision | 10,840 | **49** |
| clusters | 2,284 | 2,631 |
| oversized clusters dissolved | 1 (203 articles) | **0** |
| largest cluster | 47 articles, 1 publisher | **22 articles, 8 publishers** |

Nearly every edge was an EDGAR false merge. The size guard is now inert on this corpus —
which is the right state for it: a structural backstop that never has to fire, still there
for the day something else chains.

The cluster-size distribution is finally plausible: 2,598 singletons, 23 pairs, 2 triples,
one quad, and one real story covered by 8 publishers.

## 3.B.3 — Repairing the 2026-08-19 duplicates *(done 2026-08-20)*

`spark/jobs/repair.py`, then applied to the deployed lake.

**Deleting from the lake needs a reason, and "the table is immutable" is not a reason to keep
these.** A duplicate row is not a second observation of the world; it is one observation
recorded twice by an accounting error. The bytes are still in `bronze.raw_documents`, so the
whole table is reconstructible by replay and this destroys no record — which is the test a
destructive maintenance job should have to pass before it runs. The job defaults to
`dry_run=True` for the same reason.

It rewrites whole day-partitions rather than deleting rows. `overwritePartitions` replaces
exactly the partitions present in the DataFrame in one snapshot, so there is no window in
which the table is short a row; a DELETE followed by an INSERT would have one, and a failure
inside it would lose exactly the data the job exists to protect. Every row of every affected
partition is read back, not just the duplicated ids — an overwrite carrying only the repaired
rows would erase their partition-mates.

The survivor is the earliest `fetched_at`: the first time the pipeline actually observed the
article. Where the duplicates are byte-identical the choice is immaterial; where they differ
it is a re-fetch, and the first observation is what SPEC §6.2's "we saw it ourselves"
guarantee is about.

### Verified

Dry run, then apply, against real AWS:

| | |
|---|---|
| duplicate `article_id`s | 132, each with exactly 2 rows |
| rows | 2,849 → **2,717** |
| partitions rewritten | 3 |
| duplicates remaining | **0** — `count(*) == count(DISTINCT article_id)` |

**The brief's `exact_duplicates_removed` fell from 92 to 1.** That is the finding worth
keeping: 91 of the 92 "exact duplicates" the footer had been reporting were this defect, not
syndication. A metric that appeared to measure the world was measuring a bug in the pipeline
that computed it, and only a repair could tell the two apart.

Duplicates could not be produced through the normal path to test against — a sequential
re-run MERGEs cleanly — so `test_repair_collapses_duplicate_article_ids` writes them the way
the incident did: a bare append that bypasses the MERGE, which is what a second concurrent
writer's `WHEN NOT MATCHED` degenerates into.

## 3.B.4 — Recency measures the story, not its first report *(done 2026-08-21)*

Found by reading the brief, which is what the brief ladder is for.

Every one of the ten stories scored `recency 0.00`. Most of that is 1.D's outage — nothing
in the lake is fresher than 23 hours — but not all of it, and the remainder is a real defect
that would show on live data too.

**A cluster was timestamped by its canonical head, and the head is by construction the
earliest article** (most authoritative, then earliest seen). So a cluster's age was the age
of its *first* report. A story that keeps attracting coverage therefore looked **older the
longer it ran**, which is backwards for a brief: "this story picked up eight articles
overnight" is a strong freshness signal the design could not express at all.

Measured on the live window:

| story | first_seen | last_seen | age, head-based | age, last coverage |
|---|---|---|---|---|
| Disney sues FCC | 08-18 13:00 | 08-20 12:00 | **65.8h** | **24.0h** |
| GLM-5.3 benchmarks | 08-18 22:06 | 08-19 10:36 | 61.9h | 49.4h |
| Coinvane | 08-19 07:47 | 08-19 11:04 | 52.2h | 49.0h |

Disney drew coverage until 37 minutes before ingestion stopped and was being ranked as
nearly three days old — a 41.8-hour error on the single biggest story in the brief.

The fix carries **both** ends on the cluster, because they answer different questions:
`first_seen` is when the story broke, `last_seen` is when it was last covered, and the
ranker uses the second. Both are written to `silver.story_clusters` so 3.D can rank without
re-deriving them.

SPEC §6.2's "believe `published_at` unless it disagrees with `fetched_at`" rule moved with
it, into `dedup.trusted_timestamp`. It used to live inside `score_cluster`, where it only
ever ran against the head; it now runs **per member**, so a cluster whose head has a flagged
timestamp no longer poisons the whole cluster's age. `test_flagged_timestamp_falls_back_to_
fetched_at` was retargeted at the layer that now owns the decision rather than relaxed.

### Verified

The order changed on real data, which is the point: **NASA/Swift moved 7th → 3rd** and
**Meta AI's Mac app 9th → 4th**, both now carrying `recency 0.01` where the whole brief had
been flat `0.00`. Under the old rule the eight two-publisher stories tied at score 0.30 and
were ordered by `cluster_id` hash — arbitrarily. They are now separated by when they were
last covered.

The effect is small today only because the outage caps every story at 23 hours old. On live
data the spread is the full 0–24h range the component was designed for.

## 3.C — Entity resolution, part one: the decision and the SEC tier *(done 2026-08-21)*

Split in two the way 3.B was: the decision layer first, where `make eval` can measure it
without a JVM and without the dictionary being finished, then the second source and the
Spark tables. This is part one.

| | precision | recall |
|---|---|---|
| train (fitted on) | 0.769 | 0.370 |
| **HELD OUT** | **0.812** | **0.481** |
| full set | 0.793 | 0.426 |

`entities` was reporting *"300 labeled, awaiting a decision function to score against"* since
3.A. It now reports a number, and `evals/thresholds.toml`'s floors moved off `0.0`.

### The dictionary is a committed snapshot, not a lookup

`warehouse/entities/dictionary.json`, built by `signal_core.entities.build` from SEC's
`company_tickers.json` and a frequency-ranked English word list. Committed rather than
fetched, for three reasons that all turned out to matter: `make eval` runs in CI and no test
here touches the network; a published precision figure is only reproducible if the dictionary
it was measured against is pinned; and SPEC §9's `dim_entities` is SCD2, so a snapshot with a
`built_at` is the raw material for validity intervals while a live lookup would silently
rewrite history.

Two decisions inside it are worth naming:

**Aliases are name prefixes.** `Getty Images Holdings, Inc.` contributes `getty`, `getty
images` and `getty images holdings`. Prose says the second and SEC says the third; a
dictionary keyed on legal names alone matches neither. A **complete** name match outranks any
number of prefix matches, which is what keeps `apple` on `Apple Inc.` rather than making it
ambiguous with `Apple Hospitality REIT`.

**One company, many tickers.** 2,393 of SEC's 10,387 rows are a duplicate title — `BANK OF
MONTREAL /CAN/` appears 32 times, once per structured note it issues. SEC's file is ordered
by prominence (index 0 is NVDA), so the lowest index per CIK is the common share class. That
rule was checked against four independently hand-labeled mentions before being relied on:
`AEG` not `AEGOF`, `BMO` not `FNGD`, `CMCSA` not `CCZ`, `XRX` not `XRXDW`. 10,387 rows
collapse to 7,994 companies.

### What real data forced, and what each thing cost

Every rule below was developed against the **train half only**; the held-out half was scored
once, at the end. Four findings, in the order the measurements produced them:

**1. The first match must not win.** The sampler's spans are proper-noun runs, so they carry
headline tails: `Binance Helped Russia Target`. That span contains `target` — Target Corp, an
exact and complete company name — and the first cut linked a story about a crypto exchange to
a retailer at confidence 0.90. Scanning longest-n-gram-first does not fix it, because the
junk match is frequently the *only* match. Channels now compete and the strongest evidence
wins.

**2. A CIK is negative evidence too, and this was the single biggest precision win.** EDGAR
Form 4 and 144 filers are officers and directors filing under their own names, surname first
— and surnames start company names constantly. Measured on the train half, before the fix:

| span | resolved to | actually |
|---|---|---|
| `Matthews Mark E.` | MATW — Matthews International | a person |
| `Greene Michelle D.` | GCBC — Greene County Bancorp | a person |
| `GEE DAVID NICHOLAS` | JOB — GEE Group | a person |

The filing states the filer's CIK, and none of those CIKs belong to a company. So a CIK that
no ticker claims, on a span declaring no legal form, **vetoes** the link. A legal form
overrides the veto, because a private fund also holds a CIK no ticker claims — `PIER 88
INVESTMENT PARTNERS LLC` is a company EDGAR knows and the ticker file does not. Three false
positives out of nine, removed by reading an identifier the source had already supplied.

**3. Position is evidence.** English names are head-initial, so an alias that does not start
the span is weaker. A penalty rather than a veto, because the sampler's regex sometimes
sweeps in a leading word (`Why Apple`), and a veto would turn those into silent misses.

**4. A one-word claim is weaker than a two-word one.** `Getty Images` genuinely names Getty
Images Holdings; `carver`, `relay` and `trump` each start a real company's name and name none
of them in context. This is most of the remaining precision.

### At the fitted floor, three of the six channels are inert

Worth stating plainly, because the code reads richer than the system behaves. `CONFIDENCE_FLOOR`
fits to 0.72, and that silences everything below it:

| channel | confidence | links at 0.72? |
|---|---|---|
| CIK stated next to the span | 1.00 | yes |
| complete name, multi-token | 0.90 | yes |
| complete name, single token | 0.80 | yes |
| minted from a legal form | 0.75 | yes |
| **name prefix, multi-token** | 0.70 | **no** |
| **name prefix, single token** | 0.60 | **no** |
| **common word, corroborated** | 0.85 | never fires at all |

So the resolver that produced the numbers above is: *read the CIK, match a complete name,
or mint from a legal form.* The prefix index — the thing that makes `Getty Images` findable
at all — locates the entity and then declines to link it. That was measured, not assumed:
admitting prefix matches (floor 0.65) is neutral on the train half and, on the held-out half,
trades precision 0.812 → 0.737 for recall 0.481 → 0.519. The stated tie-break prefers the
stricter floor on equal train evidence, so 0.72 it is.

Both are kept rather than deleted, for the same reason: they are the machinery a *lower*
floor would use, and a lower floor is exactly what SPEC §7.2's embedding stage buys. Pinned
in `tests/test_entities.py` so changing either number has to be deliberate.

### The corroboration channel fires zero times, and that is recorded rather than hidden

`Meta` links if the context names `Meta Platforms` in full nearby — the lexical stand-in for
SPEC §7.2's embedding similarity. Across all 300 labeled mentions it **never fires**. It is
kept, because it is the only path by which the common-word class can ever link without
embeddings, but no published number rests on it and `CONFIDENCE_CORROBORATED` is stated
intent rather than a measured value. The same is true of `COMMON_WORD_RANK`: at the fitted
floor of 0.72 every value from 0 to 10,000 scores identically, because a common-word match
lands at 0.20 or 0.85 and never between, so the floor decides before the rank does.

### The fitting procedure had to change, and the reason is structural

3.B chose dedup's precision constraint by cross-validation inside train. **That procedure
degenerates here.** Entity precision is monotone in a single knob — raise `CONFIDENCE_FLOOR`
and you link strictly less — so "the strictest constraint whose precision survives CV" always
selects the strictest grid point. Measured: every candidate from 1.00 down to 0.80 returns
the same CV precision (0.80–0.81) and the same recall (0.183), and the rule picks 1.00, which
is a resolver that reads CIKs out of EDGAR titles and ignores prose entirely.

Dedup escapes this because its five thresholds trade against each other and because the Phase
0 fixture is a hard floor under the degenerate corner. Neither applies to a single floor. So
`ENTITY_MIN_PRECISION` is **stated in the open at 0.75** — three correct links per wrong one —
and the held-out half still reports what that choice bought.

### Verified

- `make eval` scores all three sets and gates green; `entities` floors set at 0.75 / 0.40.
- `make lint` clean, 252 tests pass, and `mypy src` clean.
- The resolver is deterministic: same inputs, same `(entity_id, confidence, method)`, which
  is what a replay of the Spark job in part two will depend on.

### Known false positives, named rather than averaged away

Three survive on the full set, and each is a different kind of hard:

- `BofA Finance LLC` → `bofa-finance`, labeled `BAC`. A financing subsidiary rolling up to its
  parent. Nothing lexical gets there; Wikidata's `P749 parent organization` is the fix, and it
  is part two's job.
- `USA Today Sparking` → `TDAY`, labeled `GCI`. A masthead whose owner is Gannett — the same
  rollup problem wearing a brand.
- `FlyWire` → `FLYW`, labeled unlinked. A fruit-fly connectome and a payments company share a
  name. No dictionary separates those; only context does.

`Lyntris Inc.` is worth recording separately because it is **not** clearly an error: the
resolver says `LYNX` via CIK, the label says `lyntris`. The company has a ticker reserved in
`company_tickers.json` and is not yet trading, so the label's namespace rule — UPPERCASE means
tradable — and the ticker file disagree about what "tradable" means. Left as a scored miss
rather than fixed on either side.

### Still open, carried into 3.C part two

- **The Wikidata tier is not built.** The dictionary is SEC-only, so `OpenAI`, `Anthropic`,
  `Substack`, `Unitree`, `Sennheiser`, `Binance`, `GitHub`, `Venmo` and `Google Drive` are all
  misses — a large, named share of the 31 false negatives. WDQS will not answer
  `?item wdt:P31/wdt:P279* wd:Q4830453` at any notability floor (**504 after 60 s**, measured
  2026-08-21), so the builder materializes the subclass closure in two steps: fetch the 5,809
  business subclasses, then fetch instances in chunks of them. The first run of that died on a
  **502** the retry set did not cover, which is now fixed alongside a smaller chunk size.
- `silver.entity_mentions` and `dim_entities` (SCD2) do not exist yet. Part two.
- Mention *detection* is still `evals/sample_mentions.py`'s lexical heuristic, which lives in
  the eval harness rather than the pipeline. Part two has to move it.

## The daily read *(SPEC §12's brief ladder; 3.F's acceptance)*

| date | read | what it showed |
|---|---|---|
| 2026-08-20 | yes | First real brief. Lead story was a 1,720-article phantom cluster — 3.0's finding, and the reason 3.B exists. |
| 2026-08-21 | yes | Ten genuine multi-publisher stories, no phantoms. Footer correctly reports the outage and names 22h of unrecoverable window-horizon loss for `rss_tech` and `rss_verge`. Surfaced 3.B.4. |

Both findings above came from reading the output, not from a test. That is the argument for
the ladder: §1's success criterion is behavioural, and a brief nobody reads is a brief whose
defects nobody finds.

### Still open

- The recall gap remains the headline number to beat: 0.500 held out. That is ADR-0009's
  question, and 3.E's.
- `dedup_ratio` is 1.02, which is honest rather than disappointing: this corpus really is
  mostly unique documents, and 17 multi-publisher stories is what `breadth` has to rank on.

## Then

3.C part two — the Wikidata tier, then `silver.entity_mentions` and `dim_entities` SCD2 on
Spark. Then 3.D wires the brief onto these tables, and 3.E ratchets the floors and writes
ADR-0009.
