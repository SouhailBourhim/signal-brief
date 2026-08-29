# Signal

*A daily tech / finance / economy brief, and the pipeline that earns it.*

Signal polls nine public sources every 5–15 minutes, stores every raw response immutably,
collapses syndicated coverage into ranked story clusters, resolves company mentions to
tickers, enriches the ranked head of each window with a locally run LLM, and mails a brief at
16:00 Africa/Casablanca — with replay, catch-up, per-run cost tracking and measured accuracy
built in rather than promised.

![What Signal is, the problem it answers, and what a morning brief actually contains](docs/assets/project-story.jpeg)

**Stack:** Python 3.12 · AWS Lambda + S3 + DynamoDB · Apache Iceberg · Spark 4 · Terraform ·
Airflow 3 · Athena · Ollama (local LLM) · Power BI

**Nine sources → bronze → silver → story clusters → ranked brief.** Ingestion is serverless
because it must run whether or not a laptop is on; processing is local Spark because EMR and
MWAA buy nothing here that `s3a` and Docker Compose do not (ADR-0002).

| | |
|---|---|
| **The hard parts** | Story-level dedup (one acquisition arrives as 40 articles) · entity resolution to tickers · a local LLM as a *governed* pipeline stage (cached, validated, quarantined, versioned) · a bitemporal macro store, because CPI gets revised for months |
| **Measured** | Dedup **1.000 / 0.500** held out (n=252) · entity resolution **1.000 / 0.593** held out (n=300) · enrichment **0.747 / 0.782** (n=95, unreviewed labels) · one brief costs **$0.0005** in Athena · the whole project cost **$0.16** over 30 days · replay after a 24.2 h outage re-read 23,306 rows and committed **0** |
| **Honest about** | Held-out numbers are quoted instead of the flattering full-set ones. Two fixes this project had already adopted on paper — embeddings for dedup, descriptions for the resolver — were later **measured and refused** by its own evidence. Bad metrics stayed published while they were bad. |

New to data engineering? [`docs/how-signal-works.md`](docs/how-signal-works.md) assumes nothing.

## Quickstart

```bash
make setup      # uv sync --all-extras + pre-commit hooks
make skeleton   # fake source -> bronze -> silver -> clusters -> HTML brief
make test
make eval       # score the labeled sets, enforce the accuracy floors
```

`make skeleton` writes `out/brief-<date>.html`. It touches **no network and no AWS account** —
it is the fastest way to confirm a change did not break the pipeline's shape.

Requires Python 3.12, JDK 17 (Spark 4), and Docker. Phases 1+ additionally need Terraform ≥
1.11 and an AWS account with the guardrails in SPEC §10.2 in place. On Windows, everything runs
inside WSL2 (ADR-0002), against a path in the WSL2 filesystem — never `/mnt/c/...`.

Against the real lake: `make up` (Airflow + Postgres), `signal brief`, `signal streak`,
`signal athena-query --sql "..."`, `signal cost`, `signal reproduce`.

## What the brief actually is

The page below is the real 2026-08-29 output, rendered to text. `make brief` builds and opens
the current one.

```
SIGNAL                                        2026-08-29 · 10 stories · day 3

  Revised since last month
  PAYEMS   2026-06-01    158984 → 158881   ▼ -103    published 2026-08-07

  Software Engineering                                              0.45 · #1
  Agentic SQL for Free: Qwen3.8 27B and DuckDB
  Qwen3.8 27B is now available for free, and it includes Agentic SQL, a
  feature that allows users to query data in a more intuitive way.
  motherduck.com · 9h ago · recency 0.65 · relevance 0.80 · novelty 0.59 · c98e41f4

  Hardware                                                          0.43 · #3
  The Galaxy Z Flip 8 is at its best when there's friction              Samsung
  Samsung has redesigned the Galaxy Z Flip 8 to be used with the cover
  screen, offering a more Android-like experience when the phone is closed.
  theverge.com · 9h ago · recency 0.61 · relevance 0.60 · novelty 0.79 · 3c95f9f0

  … 7 more …

  pipeline: ok — 1840 articles in · 1680 clusters out · dedup 1.1x · enriched 100%
            · 261.21s · 4,790,277 bytes scanned · $0.0005
  streak 3 · longest 3 · 6 briefs since 2026-08-23 · missed 2026-08-26
  hackernews ok 408 docs 2m ago · edgar ok 4 docs 2m ago · rss_tech ok 0 docs 2m ago
  edgar_formd ok 4 docs 2m · rss_verge ok 1 doc 1h · rss_ars ok 0 docs 8h
  hn_scores ok 240 docs 14m · market ok 0 docs 20h · macro ok 0 docs 20h
```

Four things on that page are the argument of the whole project:

- **The macro line is bitemporal.** June payrolls were 158,984 when first published and are
  158,881 now. Signal keeps every vintage, so "what was knowable on 2026-06-05" is a query
  rather than an archaeology project (SPEC §8).
- **The summaries and topics are a governed LLM stage**, not a prompt in a loop: one call per
  ranked cluster head, content-hash cached, Pydantic-validated, quarantined on failure, and
  scored per model digest and prompt version.
- **The score components are printed per story**, and only the ones that actually moved it.
  A single-component ranker renders as a single component instead of hiding inside six
  figures of which five read `0.00` — which is how the dead components below were found.
- **The footer is the health and cost record**, including the day the brief was missed. It is
  computed, not maintained: a hand-kept streak has exactly one failure mode, and it is that
  nobody forgets to increment it and everybody forgets to reset it.

## The shape: two boundaries, one deliberate

![The full source-to-brief journey: acquire, preserve, normalize, understand, enrich and score, assemble, deliver and learn](docs/assets/data-lifecycle.jpeg)

**Ingestion is serverless and in AWS** because it must run whether or not a laptop is on.
Nine pollers run as Lambdas on EventBridge schedules, fetching bytes and writing gzipped
JSONL to `s3://<bronze>/staging/`. A poller does not parse, filter, or interpret — it fetches and
reports how the fetch went. `FetchOutcome` distinguishes `NOT_MODIFIED` (healthy 304) from
`EMPTY` (200, nothing new) from `ERROR`, because collapsing those into "0 docs" hides the
stale-but-successful failure that monitoring exists to catch.

**Processing is local** because EMR, MSK and MWAA buy nothing here that `s3a` and Docker
Compose do not. Local Spark MERGEs staging into the `bronze.raw_documents` Iceberg table on
`ingest_id` — which is what makes replay safe by construction — then normalizes, clusters,
resolves, enriches, ranks and renders.

| Source | Gives | Cadence | Backfill horizon |
|---|---|---|---|
| Hacker News items | Community attention | 5 min | `COMPLETE` — sequential ids |
| HN top-story scores | §7.4 velocity | :07/:22/:37/:52 | `WINDOW` — score is time-dependent |
| SEC EDGAR current filings | 8-K, S-1 as filed | 15 min | `DAY` |
| SEC Form D | Private raises, often ahead of press | 15 min | `DAY` |
| TechCrunch · The Verge · Ars RSS | Curated tech coverage | 15 min | `WINDOW` — feed window only |
| Yahoo Finance daily bars | Market corroboration | 02:11 UTC | `COMPLETE` |
| ALFRED macro vintages | CPI, payrolls **with revisions** | 02:26 UTC | `COMPLETE`, all vintages |

The daily chain is ordered by Airflow **assets**, not by cron times hoped to line up
(ADR-0014): `market → macro → resolve → cluster → enrich`, with hourly `ingest_monitor`
committing bronze and `process` normalizing off that commit, maintenance at 02:00, and the
brief on a 16:00 clock. That ordering is not cosmetic — see the `recency` finding below for
what five independent crons cost.

[`docs/architecture.md`](docs/architecture.md) draws both halves and the table lineage;
[`docs/operations.md`](docs/operations.md) has the schedules, the critical path, and the
failure-to-response mapping.

## Why this isn't a news aggregator

"News aggregator with sentiment analysis" is the most-built project in this space. The
aggregation here is a couple of hundred lines. The project is the four layers around it:

1. **Story-level deduplication** — the same acquisition arrives as 40 articles.
2. **Entity resolution** — mentions to canonical companies to tickers, with measured accuracy.
3. **A local LLM as a governed pipeline stage** — cached, validated, evaluated, versioned.
4. **A bitemporal macro store** — because CPI and payrolls get revised for months.

## Measured, not claimed

SPEC §15: never publish a metric the pipeline cannot recompute. Everything below is either
reproducible with a command in this repo or cited to the runbook that recorded it.

### Quality

| Metric | Value | Source |
|---|---|---|
| Dedup precision / recall, **real pairs** | **1.000 / 0.500** held out · 0.962 / 0.568 full set (n=252) | [`evals/dedup/pairs.jsonl`](evals/dedup/pairs.jsonl), `make eval` |
| Dedup precision / recall, Phase 0 fixture | 1.000 / 1.000 (n=55) | a harness canary, not evidence |
| **Entity resolution precision / recall** | **1.000 / 0.593** held out · 0.944 / 0.630 full set (n=300) | [`evals/entities/mentions.jsonl`](evals/entities/mentions.jsonl), corrected 2026-08-29 (ADR-0018) |
| **LLM enrichment precision / recall** | **0.747 / 0.782** (n=95) · topic 0.789 · summary entailed 0.853 | [`evals/enrichment/examples.jsonl`](evals/enrichment/examples.jsonl), labels **unreviewed** |
| LLM schema-failure rate | **0%** across 100 predictions; second run 100% cache, 0 model calls | `docs/runbooks/phase-4b.md`, live Ollama |
| Entity resolution, one production window | 20,760 mentions over 4,303 articles → 2,509 linked (12.1%), 1,018 distinct companies | `spark/jobs/resolve.py`, real AWS |
| Novelty, one production window | **37.3%** of 1,680 heads are a near-exact repeat (cosine ≥ 0.99) of the prior 30 days | `evals/experiments/novelty_floor.py`, real AWS |
| Publisher breadth | **99.64%** of clusters hold exactly one publisher (1,674 of 1,680) | `signal athena-query`, real AWS |

### Throughput and latency

| Metric | Value | Source |
|---|---|---|
| Articles in, one day | 242–1,682/day, mean ~890 over 2026-08-18 → 08-28 | `silver.articles` by event date |
| Articles in → clusters out, one brief | **1,840 in → 1,680 out**, dedup 1.1x | brief footer, 2026-08-29 |
| Brief build, end to end | **261.21 s** | brief footer, 2026-08-29 |
| Ingestion freshness (source `published_at` → `fetched_at`) | **p50 3 min · p95 806 min** (n=2,621 since 08-26) | `silver.articles`, real AWS |
| …by source | HN 2/5 min · EDGAR 3/487 · Form D 111/2,851 · rss_tech 172/1,240 · rss_verge 80/1,159 · rss_ars 1,076/1,503 (p50/p95, minutes) | polls run every 5–15 min, so a p95 in hours is the source's own timestamp, not poll lag |
| End-to-end, fetch → published in the brief | **p50 9 h · p95 31 h** (2026-08-29) · p50 42–68 h on the five briefs before it | `gold.brief_items` ⋈ `silver.story_clusters` |
| Ingestion, one production window | 521 bronze rows → 207 articles (19 quarantined, all `hackernews`/dead-item) | `docs/runbooks/phase-2.md` 2.E |

The end-to-end row is the ADR-0014 effect, measured: until the daily stages were chained by
asset, `cluster` routinely ran against the previous day's bronze, and every story on the page
was already two days old at 16:00. One day of data since the fix — stated as one day.

### Cost

| Metric | Value | Source |
|---|---|---|
| **The whole project, real bill** | **$0.16** over 30 days (2026-07-30 → 08-29) ≈ $0.005/day — S3 $0.1340 · Athena $0.0169 · DynamoDB $0.0045 · Lambda $0.0000 · SNS $0.0000 | `signal cost`, Cost Explorer by `project` tag |
| Cost of one brief | **10 Athena queries, 4.79 MB scanned, $0.0005** | brief footer, real AWS |
| S3 egress | 2,489,792 bytes across 2026-08-29's 10 committed runs | `ops.pipeline_costs` |
| Compaction, before → after | **310 → 79 files** (231 removed, 74%), ~13.4 MB rewritten; the second sweep moved **0** | `docs/runbooks/phase-4a.md`, 2026-08-22 |
| Athena, `SELECT *` vs. projected vs. partition-pruned, same question | 184,259 / 73,373 / 64,713 bytes scanned | [`docs/athena.md`](docs/athena.md) |

Every Athena dollar figure floors at Athena's real 10 MB per-query minimum (`ops/athena.py`)
and currently rounds to $0.0000477 per query — the lake is still small enough that **bytes
scanned, not cost, is the metric that moves.** That is stated rather than hidden, and it is
the reason the 100× section below treats projection and pruning as a budget line rather than a
demo. The second compaction sweep moving nothing is the better half of that row: the nightly
job converges rather than churning.

### Reliability

| Metric | Value | Source |
|---|---|---|
| Consecutive daily briefs | **day 3** · 6 briefs since 2026-08-23, longest 3, **missed 2026-08-26** | `signal streak`, computed from `gold.brief_items` |
| Replay after a 24.2 h outage | 23,306 rows re-read, **0 committed**, table unchanged | `docs/runbooks/phase-1.md` 1.D, real AWS |
| Replay during active catch-up | 4 consecutive hourly MERGEs, each re-reading the full table and inserting only new rows | `docs/runbooks/phase-1.md` 1.D |
| Catch-up after the same outage | RSS lost 0.0 / 3.6 / 5.3 h of 24.2 h; HN lost nothing | `docs/runbooks/phase-1.md` 1.D |
| Local-half alerting | task failure → `signal-local-task-failed`; scheduler frozen → `signal-local-not-running` (3 missed heartbeats) | `docs/runbooks/phase-5.md` 5.A, both verified in CloudWatch |
| Refused: embeddings for same-story dedup | **4,841 false edges across 1,680 heads**, where a spanning tree needs 1,679 | `evals/experiments/embed_dedup_ollama.py`, ADR-0017 |
| Refused: entity descriptions (`?itemDescription`) | of 20 unreachable mentions, 17 are absent from the dictionary and 3 unindexed; of 6 precision errors it addresses **1** | `evals/experiments/candidate_ceiling.py`, ADR-0018 |

### What these numbers cost to earn

**The fixture's 1.000/1.000 proved the harness runs, not that the clustering is good.** On a
base-rate sample of 252 real pairs the Phase 0 rule made 34 merges and **every one was wrong**,
while missing 23 of 43 genuine same-story pairs; one cluster in the first real brief swallowed
64% of the corpus. Phase 3.B fixed both. The bad numbers stayed published while they were bad —
a metric you report only once it flatters you is not a metric — and the arc is in
[`docs/runbooks/phase-3.md`](docs/runbooks/phase-3.md).

**Entity resolution got better by getting smaller, and then by being audited.** Adding every
business Wikidata knows dropped held-out precision below the SEC-only baseline: an alias index
is only as precise as its rarest junk entry, and the subclass closure of "business" contains
every football club ever recorded. A stricter notability floor made the dictionary a third of
the size *and* better on both axes. Then ADR-0018 went through the errors one at a time and
found that **two of the six precision errors were the eval being wrong, not the resolver** —
both spans state a CIK, the resolver linked through it at confidence 1.0, and SEC's own filer
index agrees. Two more were a single bug: `has_legal_suffix` matched *any* position, so
"Investment Company Act Section" minted `investment`. Corrected, the resolver moved 0.868 →
**0.944** precision without the `?itemDescription` rebuild ADR-0009 had prescribed, and the
floors ratcheted for the first time since Phase 3.

**Reading the brief is what finds the defects.** Phase 3.D pointed the page at the real cluster
and entity tables and, with every test and both eval gates green, it was wrong four ways: a
deployed table two columns behind its DDL, a staleness warning that fired every day, a
45-article cluster merging a Disney lawsuit with a corgi tracker, and a `breadth` floor that
put nine SEC form numbers on the front page. None of them had a failing test. That is why the
count of briefs read is in the table above, and why it is computed.

**The ranker shipped at five-sixths of its spec for two phases, and the sixth component
exposed what the other five were doing.** Five of §7.4's six components read `0.00` on the real
page, and the causes were not ranking bugs:

- **`recency` read exactly `0.000` for five consecutive days** and `0.901` on the sixth — the
  day ADR-0014 replaced five crons with an asset-ordered chain. `relevance` looked saturated at
  0.94–0.98 because it was the only live term, not because it was crowding the others out; it
  fell to 0.64 the moment `recency` started competing.
- **`breadth` cannot fire in this corpus.** 99.64% of clusters hold exactly one publisher — and
  not because clustering is broken: all six multi-publisher clusters in a window are correct
  ars/verge/techcrunch/HN merges. 64% of the corpus is single-publisher SEC filings and 30% is
  Hacker News pointing at 477 distinct domains. A quarter of the score was resting on a signal
  this source mix emits for 0.36% of clusters. It is now 0.05, and goes back up when wire
  sources land — with the measurement that raises it.
- **One feedback mark existed across 60 items**, and the cause was the product, not the reader:
  marking needs a `cluster_id` and the page printed none, so the loop required copying a
  64-character hash out of a table nobody could see. The card now prints eight characters and
  `signal feedback` resolves a prefix.

**Three caveats carried on purpose.** *Quote the held-out row*: half of each full set was
fitted on, so 0.962 and 0.944 are optimistic by construction, while 1.000 / 0.500 and
1.000 / 0.593 were measured on examples the fitting never saw. *Parts of the resolver are inert
at the fitted floor* — prefix matching and context corroboration both locate an entity and then
decline to link it — pinned in tests rather than left to imply the system does more than it
does. And *the labels were made by an LLM assistant*: the dedup and entity sets were then
reviewed by the reader, who overrode three (`labeler` is stamped on every record,
`reviewed_from` on every overridden one); **the 95 enrichment labels are stamped `unreviewed`**,
so 0.747/0.782 is a model grading a model and must not be read as ground truth.

**The enrichment number's stratified draw earned its keep on first use.** SEC filings score
**1.000** on topic and get `filing_type` wrong 12 times in 20 — the model reads the SEC *Item*
number off the body when the form type is the first token of the title. A uniform draw (57%
filings) would have published a topic accuracy near 0.9 earned on the trivially classifiable
half. `company` false-positives 17 times and every one is the publisher domain read as the
subject. `round_type` fires zero times in 95 — the abstention `enrich/schema.py`'s nullability
was written for, measured rather than assumed.

**Two experiments deliberately are not runnable by `make eval`.** Deciding whether a 1.1 GB
dependency earns its place by first adding it to `pyproject.toml` would answer the question by
assumption, so `evals/experiments/` runs against a throwaway virtualenv and each script carries
the command that builds one. Both imported their split, objective and constraint selection from
`evals/fit_thresholds.py` rather than choosing friendlier ones.

**The last thing Phase 3 measured was its own fitting procedure.** Asked to re-derive the
same-story constants, `evals/fit_thresholds.py` returned a configuration disagreeing with the
shipped code in three places and recommending exactly the value 3.D had removed for merging a
Disney lawsuit with a corgi tracker. Nothing had failed, because a fitter's output is prose
until someone reads it. The cause: of the four constants in the grid, **252 labeled pairs
determine one**, and 336 of 385 feasible grid points tie at the top score, so a tuple-order
tiebreak was silently choosing the rest — badly. The fit now breaks ties by keeping what ships,
reports which constants the labels leave free, and is pinned by tests.

## Replay and catch-up are different promises

- **Replay** — reprocess an interval from bytes already in `bronze/`. Deterministic, no
  network, always available. The MERGE on `ingest_id` makes it true by construction.
- **Catch-up** — re-fetch what was missed during downtime. Bounded by each source's
  **backfill horizon**, and partial by construction for RSS: items that rotated out of the
  feed are gone. What cannot be recovered is written as a `gap_reason` per source per interval
  into `ops.source_health` and printed in the brief footer, rather than left to look like a
  quiet day (SPEC §6.3).

The horizon is measured backward from *now*, not from the outage's end — an outage noticed
three days late has a three-day reach problem, not a one-hour one.

### Measured, on the deployed pipeline

All six pollers then deployed were disabled for **24 h 11 m** (2026-08-20T12:37:25Z →
2026-08-21T12:49:15Z) and
the pipeline was watched through the outage and the recovery
([`docs/runbooks/phase-1.md`](docs/runbooks/phase-1.md) 1.D).

**Replay held completely.** Re-running the commit over the stored interval read all 23,306
staged rows and inserted **0** — `committed_rows: 0`, `duplicate_rows: 23306`, `table_rows`
unchanged, `egress_bytes: 0`. Twelve idle hourly runs during the outage reproduced it twelve
more times.

**Catch-up held partially, and the split is per-source:**

| Source | Horizon | What a 24.2 h outage actually cost |
|---|---|---|
| `hackernews` | `COMPLETE` | **Nothing.** 13,581-item backlog drained at ~1,520 items/hour net — fully caught up in ~8.7 hours |
| `edgar`, `edgar_formd` | `DAY` | **Nothing**, by 11 minutes. The horizon reaches back 24 h; the outage ran 24.2 h |
| `rss_ars` | `WINDOW` | **0.0 h lost** — 20-item feed at ~0.46 items/hour, so its 20 slots span ~45 h |
| `rss_tech` | `WINDOW` | **3.6 h lost** (~8 articles) — all 20 pre-outage items had rotated out |
| `rss_verge` | `WINDOW` | **5.3 h lost** (~3 articles) — 10-item feed, all 10 rotated out |

The thing worth knowing is **why** the RSS numbers vary: these feeds hold a fixed *count* of
items, not a fixed *duration*, so reach in hours is inversely proportional to how fast the
source publishes — and it collapses exactly when the source is busiest, which is when the
missed items matter most. `rss_tech` published 20 items in the 8.3 hours it was active; at that
rate a 24-hour outage across a full news day would have cost ~16 hours, not 3.6. **RSS loss is
rate-dependent and unbounded above.** The measured range here is 0–5.3 h; it is not a ceiling.

The pipeline's own `gap_reason` reported `22.6h unrecovered` for all three RSS sources against
measured losses of 0.0/3.6/5.3 h. `HORIZON_REACH[WINDOW]` is a deliberately conservative flat
1 hour, so the reported gap is an **upper bound on loss, never an under-report** — the right
direction for an operational alert, and stated here next to the measurement rather than in
place of it.

## A decision I reversed

SPEC §16.8 names the Kafka cut as the raw material for this section. The two after it are
sharper, because they reverse a decision this project made and measured itself.

**Kafka was cut before anything was built** (ADR-0001). The sources are micro-batch by nature:
they emit on a 5–30 minute cadence and are reached by polling, so a topic would manufacture
streaming semantics the data never had, sitting between one local Spark job and another with
`bronze/` already the replay source of truth. The re-entry criterion is written down and was
measured on 2026-08-29: a genuinely continuous source **and** a second independent consumer
with its own latency SLA. **9 polled sources, 0 continuous; 9 readers of `silver.articles`, 0
with an SLA.** Neither clause is close (ADR-0015).

**Embeddings for same-story dedup were adopted, then refused — by this project's own
measurement.** ADR-0009 adopted them on a `sentence-transformers` trial (held-out recall
0.500 → 0.909), then rejected `sentence-transformers` as the vehicle on packaging grounds.
ADR-0016 built the vehicle it chose instead — `nomic-embed-text` through Ollama, pinned by
digest. Asking the question again through the encoder that would actually ship **refused it**
(ADR-0017): on the 252 labeled pairs no threshold at or above 0.90 changes a single decision,
and the 19 positives the lexical rule misses sit *underneath* the hardest true negatives. The
only threshold that buys recall (0.85: 0.568 → 0.705) emits **4,841 false edges across 1,680
heads** in one window, where a spanning tree of 1,680 nodes needs 1,679 — Phase 3.B's 59%
mega-cluster arriving again, on paper instead of in a morning's brief.

The first measurement was not careless. It was taken on the labeled set alone — the axis 3.B
had already shown to be insufficient. The merges land on templated filing titles a retrieval
encoder is *correct* to call similar, in a corpus that is 64% filings. **ADR-0009 §1 is
reversed; §2 is confirmed.** The same encoder is in production for `novelty`, where the
property that ruins dedup is exactly what is wanted.

**A third reversal came out of the same discipline.** ADR-0009 §2 named `?itemDescription` as
the resolver's fix. ADR-0018 measured it before touching the query: of the 20 unreachable
mentions, 17 are absent from the dictionary and 3 are unindexed — a description arbitrates
*between candidates*, and a mention proposing none has nothing to arbitrate. It addresses one
of the six precision errors. Refused; the recall ceiling of 0.630 turned out to be a property
of one channel, not of the resolver.

## What I'd do differently at 100× volume

100× is ~90k articles/day instead of ~900. Five things break, in this order, and none of them
is the part that usually gets rewritten first.

1. **Poller fan-out, not poller code.** Six of the nine pollers already collide at
   :00/:15/:30/:45; source #7 was the point where a new account's concurrency limit of 10
   stopped fitting, and it was
   dodged by phase-shifting schedules into cron (`:07/:22/:37/:52`, `02:11`, `02:26`). That
   trick is exhausted at 100×. The fix is Service Quota L-B99A9384 and a queue between
   *discovering* item ids and *fetching* bytes, so one slow source cannot eat an invocation
   that is walking 200 sequential ids.
2. **Small files, before anything else.** Every fetch writes one gzipped object; nightly
   compaction already reclaims 74% of files (310 → 79). At 100× that stops being hygiene and
   becomes what governs query cost, so it moves to hourly per-partition compaction — and
   `remove_orphan_files`, currently a documented skip because `hadoop-aws` is not on the
   classpath (ADR-0006's jar-conflict argument), stops being acceptable.
3. **Clustering's union-find leaves the driver.** Blocking is already exact and cheap —
   rarest-token prefix filtering, with oversized keys dropped and *counted*
   (`blocking_keys_dropped`) rather than silently swallowed. What does not scale is
   `group_edges` running connected components in one process. At 100× that becomes an
   iterative Spark join, and the pgvector gate — measured at **11,267 vectors against a 50k
   criterion**, reached around 2026-12 at the current rate — actually opens.
4. **The Athena cost model inverts.** Today every query floors at the 10 MB minimum, so bytes
   scanned is the metric that moves and dollars are noise. At 100× the measured spread —
   184,259 vs. 73,373 vs. 64,713 bytes for the same question — is the difference between a
   $0.005/day bill and a real one, so projection and partition pruning stop being a
   demonstration in `docs/athena.md` and become a budget the ranker's queries are held to.
5. **The LLM stage is the thing that does *not* scale with the corpus**, and that is the
   design property worth stating: enrichment runs on the *ranked head*, after selection, so
   100× the articles is still ~40 calls a night at 2.04 s each. What breaks first is the single
   local GPU as a serialization point, not the token bill — and the honest answer there is that
   free inference stops being free the moment it has to be concurrent.

What would *not* change: Kafka. Its criterion is a continuous source and a second consumer, not
volume — 100× of polled sources is still polled. Local Spark is the one that would go, and it
would go on a measurement (ADR-0002's own consequence is that Spark's memory is bounded by the
Airflow worker process), not on taste.

## Where this stands

**Phases 0 through 5 are built and deployed.** Nine pollers run on schedule and land raw
payloads in S3; local Spark commits them to Iceberg, normalizes, clusters, resolves entities,
enriches through Ollama and ranks; the brief has gone out at 16:00 on six of the seven days
since 2026-08-23, with the missed one named on the page; the
infrastructure is Terraform, and applied. All three labeled sets are scored in CI on every PR,
against floors that ratchet up and never down.

| Phase | What it added | State |
|---|---|---|
| 0 | Walking skeleton — fake source → bronze → silver → clusters → HTML, no network | Done |
| 1 | Real pollers, staging → Iceberg MERGE, state, monitoring, **replay and catch-up measured against a deliberate 24.2 h outage** | Done |
| 2 | The lake: normalize, quarantine, Athena, cost tracking, the query measurements | Done |
| 3 | Story clustering and entity resolution, both **labeled and scored**, plus the fitting procedure that audited itself | Done |
| 4A | The ranker, the 16:00 mail, nightly maintenance, the feedback CLI | Built; acceptance is three mornings read with marks recorded — calendar |
| 4B | The governed LLM stage and the bitemporal macro store | Done; enrichment scored 2026-08-29 |
| 5 | Computed streak, local-half alerting, §14's five deferrals **measured and refused**, novelty + the weight rebalance, Power BI as a reader | Done; gate is 14 consecutive briefs, which lands 2026-09-05 |

**Open, and named rather than quietly dropped:**

| Item | Why it is open |
|---|---|
| 14 consecutive briefs | Calendar. Day 3 of 14; 2026-09-05 at the earliest |
| A human review pass over the 95 enrichment labels | They are stamped `unreviewed`; the dedup and entity sets were reviewed and three were overridden |
| 4A's three mornings read, with a mark | Calendar and a reader |
| The 30-day reproducibility backfill | Bronze starts 2026-08-18, so `signal reproduce --days 30` opens **2026-09-17** |

Each phase's runbook records **what broke on first real use**, which is usually the more useful
half: [`docs/runbooks/`](docs/runbooks/).

## Adding a source

The design's central claim is that this takes 30 minutes:

1. Write `src/signal_core/sources/<name>.py` implementing
   `poll(config, state) -> (list[RawDocument], State)`.
2. Register it in `src/signal_core/sources/__init__.py` and `config.SOURCES` — declaring its
   **backfill horizon**, which determines what catch-up can honestly promise (SPEC §6.3).
3. Write `src/signal_core/parse/<name>.py` — usually a one-line binding of
   `feedparse.parse_feed` for RSS/Atom — and register it in `parse/__init__.py`. Without this,
   the source polls and commits to bronze fine and then fails silently on the silver side the
   first time `normalize_window` runs.
4. Add one entry to the Terraform `sources` map.

`tests/test_source_registry.py` asserts all four places agree, so a missed step fails a test
rather than a Lambda at 3am.

## Layout

| Path | Contents |
|---|---|
| [`SPEC.md`](SPEC.md) | The specification and source of truth. Start here |
| [`src/signal_core/contracts.py`](src/signal_core/contracts.py) | The poll contract every source implements |
| `src/signal_core/` | Sources, transform, dedup, entities, Spark jobs, enrichment, ranking, rendering, ops |
| `warehouse/entities/` | The pinned SEC + Wikidata dictionary the resolver is measured against |
| `handlers/` | Lambda entry point — one artifact, N functions |
| `infra/terraform/` | `bootstrap/` (state backend), `main/` (everything else) |
| `evals/` | Labeled sets, scorers, and the accuracy floors CI enforces |
| `evals/experiments/` | Measurements that decide a question without shipping a dependency — the embedding trials, the corpus-level false-merge rate a pairwise eval cannot see, the novelty floor |
| `analytics/powerbi/` | The read-only query set behind ADR-0012 |
| [`docs/architecture.md`](docs/architecture.md) | What runs where, why the AWS/local line falls there, table lineage, what is deliberately missing |
| [`docs/operations.md`](docs/operations.md) | How Signal runs day to day: the 16:00 critical path, replay vs. catch-up, failure-to-response |
| [`docs/athena.md`](docs/athena.md) | Querying the lake, and the `SELECT *` vs. projected vs. pruned measurement |
| [`docs/how-signal-works.md`](docs/how-signal-works.md) | Every phase in plain English — no prior knowledge assumed |
| [`docs/runbooks/`](docs/runbooks/) | What was actually done per phase, including what broke on first real use |
| [`docs/decisions/`](docs/decisions/) | ADRs, including the three that reversed earlier choices |

## License

MIT
