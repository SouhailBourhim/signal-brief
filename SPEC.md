# Signal — Specification

*A daily tech / finance / economy brief, and the pipeline that earns it.*

**Version 3 — reconciled.** Supersedes `docs/archive/signal-design.md` (v2, the ambitious spec)
and `docs/archive/PROJECT_START.md` (the narrowed execution plan). Those two disagreed on scope in five places;
§0 records how each conflict was resolved and why. This document is the one to build from.

---

## 0. Reconciliation decisions

| Conflict | v2 design said | PROJECT_START said | **Resolved** |
|---|---|---|---|
| Bitemporal macro (ALFRED) | Core differentiator (§8) | Explicitly deferred | **Kept, scheduled as Phase 4B** (Phase 4 until ADR-0008 split it). It is one of the four things that stop this being an aggregator; deferring it indefinitely guts the README's lead. It is late, not optional. |
| Kafka | Core of the architecture | Kept in MVP, but its consumer was deferred | **Cut from Phases 1–4. Re-entry criteria in §14.** In the MVP it sat between a local Spark job and a local Spark job, with `bronze/` already the replay source of truth — a topic with no independent consumer and no process boundary. |
| Spark Structured Streaming | Core (consumes Kafka) | Deferred | **Deferred with Kafka.** Batch Spark on the 15-minute cadence is the same code and the same correctness story without the exactly-once argument. |
| dbt | In the stack table | Deferred | **Phase 5.** Gold marts are hand-written SQL until there are enough models for dbt's tests and lineage to pay for themselves. |
| pgvector | In the stack table | Deferred | **Deferred, with arithmetic (§14).** The working set is ~1k–3k vectors; that is a numpy array, not a database. |
| Sources at start | Start with 5 | MVP with 3 | **3 in Phase 1, 5 by end of Phase 2.** Three proves the adapter contract; the fourth and fifth prove it was worth having. |
| Phase count | 4 | 5 | **5** (v2's Phase 1 splits into ingest and lake/query). Since amended: Phase 0 was always real but unlisted, and ADR-0008 split Phase 4 into 4A/4B — see §12 for the current shape. |
| Repo layout | `spark/`, `enrich/`, `dbt/` | `processing/`, `streaming/`, `warehouse/` | **Single tree in §13.** |
| Backfill acceptance test | "reproduces byte-identical output" | "reproducible from raw S3 objects" | **Rewritten (§12).** Byte-identical is not achievable across incremental clustering and an LLM stage, and claiming it invites exactly the question you cannot answer. |
| "Replay" | Used for two different things | Same | **Split into replay vs catch-up (§6.3).** They have different guarantees and only one of them is always possible. |

---

## 1. What it is

Every 15 minutes, ingest free news, filing, and macro feeds. Collapse thousands of articles into a
few dozen **story clusters**. Resolve the companies mentioned to real tickers. Rank clusters by
novelty, breadth, velocity, and whether the market actually reacted. Publish a brief at **07:00
Africa/Casablanca** that you read every morning and mark up.

**Success is behavioural, not technical:** the project works when you read it daily for a month
without maintaining it. Everything below exists to make that true.

AWS is the initial infrastructure implementation, not the point of the project.

---

## 2. Why it isn't a news aggregator

The aggregation is roughly 200 lines. The project is the four layers around it:

1. **Story-level deduplication** — the same acquisition arrives as 40 articles.
2. **Entity resolution** — mentions to canonical companies to tickers, with measured accuracy.
3. **A local LLM as a governed pipeline stage** — cached, validated, evaluated, versioned.
4. **A bitemporal macro store** — because CPI and payrolls get revised for months.

"News aggregator with sentiment analysis" is the most-built project in this space. The README leads
with those four, with numbers attached. Lead with a dashboard screenshot and it disappears into
the pile.

---

## 3. Sources

`backfill horizon` is how far back a source can be re-fetched after downtime. It is not a detail —
it determines what §6.3's catch-up can honestly promise, and it varies enormously.

| Source | Gives | Cadence | Backfill horizon | The interesting difficulty |
|---|---|---|---|---|
| RSS: TechCrunch, The Verge, Ars | Curated tech coverage | 5–30 min | **Feed window only (~1–3 h)** | `pubDate` often wrong or absent; feeds go stale while still returning 200 |
| [Hacker News API](https://github.com/HackerNews/API) | Community attention, score velocity | 1 min | **Complete** (sequential item ids) | Score is time-dependent — snapshot it, never overwrite |
| [SEC EDGAR RSS](https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent) | 8-K, S-1, Form D as filed | near real time | **~1 day** via current feed; full via daily index | Requires descriptive `User-Agent` with contact email; fair-access rate limits |
| SEC **Form D** | Startup fundraises, often ahead of press | daily | **~1 day** via current feed; complete via daily index | Underused; a genuine scoop source |
| [FRED / ALFRED](https://fred.stlouisfed.org/docs/api/fred/) | Macro series **with vintages** | per release | Complete, all vintages | Revisions are the point — see §8 |
| [GDELT 2.0](https://www.gdeltproject.org/) | Global news stream, tone + entity annotations | 15 min | Complete (file archive) | High volume, noisy, multilingual, files occasionally late or empty. **Read §10 on egress before enabling.** |
| ECB / Fed / BoE press RSS | Central bank statements | irregular | Feed window | Long docs; value is in the diff against the previous statement |
| Stooq / yfinance | Prices, indices, FX | daily | Complete | Splits and dividends retroactively rewrite history |
| arXiv (cs.LG, econ) | Research signal | daily | Complete | v1→v2 are updates, not new papers |

**Phase 1 (3):** one tech RSS feed, Hacker News, SEC EDGAR current filings.
**Phase 2 (6):** add SEC Form D and *two* more RSS publishers — The Verge and Ars Technica.
Everything below the line is Phase 4+.

Six rather than the five this section originally planned, for one reason: the constraint below
is about source #6 specifically, and a constraint nobody reaches is a constraint nobody has
tested. Ars Technica is that sixth source.

Adding source #6 must be a 30-minute job — that constraint drives §6.
**Measured 2026-08-19: 16 minutes for all three of Phase 2's sources**, including the Terraform
apply and live verification against the real endpoints. `docs/runbooks/phase-2.md` records it.
`tests/test_source_registry.py` is what keeps it true, by failing the build when a source is
declared in some but not all of `SOURCES`, `sources.REGISTRY`, and Terraform's `var.sources`.

---

## 4. Architecture

```
                    ┌─────────────── AWS (always-free tier) ───────────────┐
                    │                                                       │
  feeds ──────────► │  EventBridge Scheduler ──► Lambda poller (1 per src)  │
                    │            │                                          │
                    │            ├──► S3  bronze/  (raw, immutable)         │
                    │            └──► DynamoDB  (etags, watermarks, seen)   │
                    │                                                       │
                    │  Glue Data Catalog ◄── Iceberg metadata               │
                    │  Athena ──────────────► ad-hoc + serving queries      │
                    └───────────────────────┬───────────────────────────────┘
                                            │  s3a, read-once local cache
                    ┌─────────────── local (docker compose) ────────────────┐
                    │                       ▼                                │
                    │   Spark batch: normalize ──► silver.articles           │
                    │                       │                                │
                    │   Spark batch: simhash/LSH ─► embed ─► cluster (72 h)  │
                    │                       │                                │
                    │              entity resolution (dict + context)        │
                    │                       │                                │
                    │              Ollama enrichment (cached, validated)     │
                    │                       │                                │
                    │              ALFRED vintages ──► macro_observations    │
                    │                       ▼                                │
                    │      Iceberg tables on S3, registered in Glue          │
                    │                       │                                │
                    │              ranker ──► brief renderer                 │
                    └───────────────────────┬────────────────────────────────┘
                                            ▼
                         static HTML + email, 07:00 Africa/Casablanca
                                            │
                                 feedback ──┘ (stored; see §7.4)

  Airflow (local) orchestrates everything, including the AWS-side resources.
```

Two boundaries, deliberately: **ingestion is serverless and in AWS** because it must run whether or
not your laptop is on; **processing is local** because EMR, MSK, and MWAA buy nothing here that
`s3a` and Docker Compose do not.

---

## 5. Stack

| Layer | Choice | Rationale |
|---|---|---|
| Orchestration | **Airflow 3** | 29% of postings; Dagster's asset model is nicer but barely appears in job ads |
| Compute | **PySpark**, batch | 33% of postings, largest single salary premium (~+$20K) |
| Table format | **Apache Iceberg** on S3 | 58% of surveyed enterprises use it for business-critical workloads; 79% migrating more within a year |
| Catalog | **AWS Glue Data Catalog** | First 1M objects + 1M requests/month free; the standard AWS Iceberg catalog |
| Ingestion | **Lambda + EventBridge Scheduler** | Always free at this volume; genuinely serverless |
| Pipeline state | **DynamoDB** | 25 GB always free; right shape for watermarks and seen-sets |
| Query / serving | **Athena** | $5/TB scanned, 10 MB minimum — cents monthly here, zero infrastructure |
| Inference | **Ollama** (8B-class, pinned digest) | Zero marginal cost, and forces real thinking about determinism |
| Embeddings | **sentence-transformers**, local | Same |
| IaC | **Terraform** | 14%; also what stops the project smelling console-driven |
| CI | **GitHub Actions** | CI/CD appears in 30% of postings and costs an afternoon |

**Deferred with re-entry criteria (§14):** Kafka, Spark Structured Streaming, dbt, pgvector.
**Excluded outright:** Scala, Hadoop, Kubernetes, EMR, MSK, Glue ETL jobs and crawlers, RDS,
OpenSearch, Redshift (except the time-boxed evaluation in §10).

---

## 6. Ingestion

### 6.1 The contract

**One Lambda per source**, all conforming to a single contract, because adding source #6 must be
trivial:

```python
def poll(source_config: SourceConfig, state: State) -> tuple[list[RawDocument], State]
```

The adapter's only job is to fetch and hand back bytes plus metadata. It does not parse, normalize,
or filter. Everything interpretive happens in Spark, where it can be re-run against stored bytes.

### 6.2 Rules

- **Raw payloads are immutable and never re-fetched or overwritten.** Everything downstream is
  recomputable from `bronze/`.
- **Conditional requests** — persist `ETag` / `Last-Modified` in DynamoDB, send `If-None-Match`.
  Most feeds return 304 most of the time; this is the difference between polite and rate-limited.
- **Fetch metadata is data** — status code, latency, byte count, and `fetched_at` are stored
  alongside the payload. §11's monitoring is built entirely from these.
- **Trust no timestamp.** Record `fetched_at` (certain) separately from `published_at` (claimed).
  Where they disagree beyond a threshold, flag rather than silently correct.
- **Per-source rate limiting and a descriptive `User-Agent`.** SEC requires a contact email in it;
  getting blocked by EDGAR mid-project is an avoidable afternoon.
- **Failed records are quarantined with a reason.** Never silently dropped.
- **Every bronze object is downloaded to local at most once**, into a content-addressed cache keyed
  by object ETag. This is a correctness convenience and a cost control — see §10.

### 6.3 Replay and catch-up are different things

The two documents this supersedes used "replay" for both. They have different guarantees:

- **Replay** — reprocess an interval from bytes already in `bronze/`. Always possible, fully
  deterministic for the stages in §12's acceptance test, and the real guarantee the project makes.
- **Catch-up** — after downtime, re-fetch what was missed from the source. **Bounded by each
  source's backfill horizon (§3).** For RSS this is partial by construction: items published and
  rotated out of the feed during the outage are gone, and no amount of engineering recovers them.

State this distinction in the README rather than letting an interviewer find it. Catch-up records a
`gap_reason` per source per interval so the loss is visible in `ops.source_health`, not implied.

### 6.4 Bronze layout

```
s3://signal-bronze/source={source_id}/ingest_date={yyyy-mm-dd}/hour={hh}/*.parquet
```

Iceberg table `bronze.raw_documents`, partitioned by `(source_id, ingest_date)`.

---

## 7. Processing

### 7.1 Deduplication and story clustering

Four stages, cheapest first:

1. **Exact** — content hash after boilerplate stripping.
2. **Near-duplicate** — 64-bit simhash with banded LSH. Catches syndication and light rewrites.
3. **Same-story clustering** — sentence-transformer embeddings, incremental clustering over a
   time-decayed 72-hour window. "Apple acquires X" and "X to be bought by Apple" are one event.
4. **Canonical selection** — earliest credible publisher becomes cluster head; the remainder become
   `distinct_publisher_count`, which feeds ranking rather than being discarded.

Clustering is **order-dependent by construction** — assignments depend on arrival sequence within
the window. Record the input ordering key with each run so a replay can reproduce it, and see
§12 for what the acceptance test therefore claims.

**Publish:** dedup ratio (articles in ÷ clusters out) and precision/recall against ~200 hand-labeled
article pairs. The labeled set lives in `evals/` and is committed.

**As built** (ADR-0009): stages 1, 2 and 4 ship; stage 3 ships as a lexical rule — titles against
titles, bodies against bodies, never pooled — and the embeddings land in 4B. Measured against the
252 real pairs, embeddings roughly double held-out recall (0.500 → 0.909) at a bounded precision
cost that does not chain at corpus scale, so this is a scheduling decision and not a rejection.
Banded LSH is also not shipped: stage 2 is exact simhash equality, because on *this* corpus
(`dedup_ratio` 1.01, almost no true syndication) every looser distance was a collision source and
no measured gain. Both would be worth re-asking on a corpus with real newswire reprints.

### 7.2 Entity resolution

Dictionary built from SEC `company_tickers.json` plus Wikidata aliases. Then the hard cases: "Meta"
versus metadata, Apple Inc. versus an apple supplier, subsidiaries rolling to parents, private
companies with no ticker, and **companies that rename** — which is an SCD2 problem wearing a
disguise, handled in `dim_entities` with `valid_from` / `valid_to`.

Disambiguation by cosine similarity between article context and entity description embeddings, with
a confidence floor below which a mention is left **unlinked rather than guessed**.

**As built** (ADR-0009): the confidence floor ships and is fitted; the embeddings do not, and the
reason is not the dependency. The dictionary carries names, tickers, CIKs and aliases and **no
descriptions**, so there is nothing to embed on the entity side — and the alias index proposes the
correct entity for only 34 of 54 linked mentions, capping any context-scoring rule at 0.630 recall
against 0.611 shipped. The fix is `?itemDescription` in the WDQS projection and a wider candidate
set, in that order, before an encoder is worth reaching for.

**Publish:** precision/recall over 300 hand-labeled mentions. Very few portfolio projects quantify
this, which is exactly why it reads as senior.

### 7.3 LLM enrichment (Ollama)

Three jobs per cluster: a one-sentence summary, topic classification, and structured extraction
(company, amount, round type, headcount delta, filing type) into a typed schema.

Governed like any other transform:

- **Content-hash cache** keyed on `(input_hash, model_digest, prompt_version)` — never re-infer.
  Cache hit rate is a published metric.
- **Pydantic validation** on every output. Failures are quarantined to `gold.enrichment_rejects`,
  never silently dropped, and never retried indefinitely.
- **Eval harness** — 100 labeled examples in `evals/`, accuracy tracked per model and prompt
  version, so swapping models is a measurement rather than a vibe.
- **Determinism boundary** — temperature 0, pinned model digest, versioned prompts, and everything
  downstream marked non-reproducible in lineage. Being explicit about this is the mature move.
- **Capacity, stated plainly** — enrichment runs against **cluster heads once per pre-brief window,
  not on every 15-minute cycle**. Measured on the dev box (RTX 5070 8GB, `llama3.1:8b` q4, see
  ADR-0003): one-time model load ~23 s, then ~1.0 s/head at ~60 tok/s steady state. A 40-head batch
  is therefore **~1 minute**, comfortably inside the window — well under the ~3-minute figure this
  paragraph assumed before the model was pinned and measured. The DAG asserts this bound and fails
  loudly rather than silently lagging the 07:00 send.

### 7.4 Ranking

A brief is useful because of what it **omits**. Score each cluster:

| Component | Signal |
|---|---|
| Novelty | Embedding distance to the last 30 days of clusters — recycled narratives sink |
| Breadth | Count of *independent* publishers (§7.1 makes this honest) |
| Velocity | HN score slope, mention-rate acceleration — **blocked on a source change**, see below |
| Relevance | Your watchlist of companies, technologies, macro series |
| Market corroboration | Did the linked ticker or rate move beyond its normal range? |
| Feedback | Your morning thumbs up/down |

**Velocity is not currently buildable, and that is a source-design problem, not a ranking one.**
`sources/hackernews.py` walks item ids forward from a watermark, so every item is fetched exactly
once — at creation, when its score is 1 and it has no comments. §3 says "snapshot it, never
overwrite"; there are no second snapshots to slope against. A second poller over `topstories.json`
that re-fetches and snapshots current scores is what unblocks it, and it is listed in §12's
carried-forward table for 4A. Until it lands, velocity stays out of `WEIGHTS` rather than being
approximated by something that is not velocity. (Comment arrival rate per story *is* derivable from
single-fetch data — which is part of why `silver.hn_comments` earns its place; see
`docs/runbooks/phase-2.md`.)

**Weights are hand-set and stay hand-set.** One reader's daily marks are instrumentation, not a
training set — a weekly refit on tens of labels overfits to last week's mood and is indefensible in
an interview. Store `score_components` as a map so every ranking decision is explainable after the
fact, and revisit automated fitting only if the feedback table passes several hundred marked items.
The brief ladder (§12) is what makes that threshold reachable rather than theoretical: marks start
accumulating in Phase 3, not at the end of Phase 4. The deliverable here is explainability, not
learning.

---

## 8. The bitemporal macro store

**Phase 4B. Not optional** — this is differentiator #4 and the README leads with it.

Macro data is revised for months after first publication. A normal pipeline overwrites and quietly
destroys the record; this one keeps two time axes:

- `valid_time` — the period the number describes
- `known_time` — the vintage date it was published

so "what was knowable on 2026-03-14" is a query, not an archaeology project.

Two payoffs. The brief can state **"payrolls revised down 46k across the prior two months"** — a
thing coverage routinely buries and which often matters more than the headline print. And
bitemporal modeling against genuinely revising data is a senior warehouse skill that arrives here
naturally instead of being bolted on for show.

ALFRED serves every vintage, so this is backfillable from a standing start — no history is lost by
building it late, and it depends on nothing in Phases 2-3.

That argument was originally used to justify putting it in Phase 4, and it only ever covered *data*
risk. **Schedule risk is the opposite** (ADR-0008): sharing a row with the ranker, the renderer,
the mailer and the maintenance DAG is what put it at risk, because when a ten-item phase runs long,
the plumbing feels mandatory and the differentiator is what slips. Hence 4B, where the two things
the README leads with — this and §7.3's governed LLM stage — are the whole phase and cannot be
crowded out by anything else.

---

## 9. Data model

**Bronze**
- `raw_documents` — `ingest_id, source_id, fetched_at, source_url, http_status, etag, content_hash, payload, payload_format`

**Silver**
- `articles` — `article_id, source_id, url_canonical, title, body_text, published_at, fetched_at, event_date, lang, publisher_domain, authority_score, simhash, content_hash`
  (plus `timestamp_flagged`, `story_key`, `parse_error` — operational columns predating this
  section, carried here for completeness rather than re-litigated)
- `hn_comments` — `item_id, parent_id, story_id, by, text, created_at, fetched_at, ingest_id,
  dead, deleted`. Hacker News comments only — not `articles`; see ADR-0007 and
  `docs/runbooks/phase-2.md`'s velocity finding for why they get their own table
- `parse_rejects` — `ingest_id, source_id, parse_error, fetched_at, rejected_at`. A bronze row
  that failed to parse at all (SPEC §6.2 quarantine, at the row granularity — a single bad
  entry inside an otherwise good feed stays in `articles` with its own `parse_error` instead)
- `story_clusters` — `cluster_id, first_seen_at, last_updated_at, canonical_article_id, article_count, distinct_publisher_count`
- `article_cluster_map` — `article_id, cluster_id, similarity, assigned_at, assignment_order_key`
- `entity_mentions` — `mention_id, article_id, surface_form, char_span, entity_id, confidence, resolution_method`
- `dim_entities` (SCD2) — `entity_id, canonical_name, ticker, cik, entity_type, valid_from, valid_to, is_current`

**Gold**
- `cluster_enrichment` — `cluster_id, model_name, model_digest, prompt_version, summary, topic, extracted_json, generated_at, input_hash, cache_hit`
- `enrichment_rejects` — `cluster_id, input_hash, raw_output, validation_error, model_digest, prompt_version, rejected_at`
- `macro_observations` (bitemporal) — `series_id, period, value, vintage_date, is_latest, revision_delta`
- `brief_items` — `brief_date, rank, cluster_id, score, score_components, included, user_feedback`

**Ops**
- `source_health` — `source_id, window_start, docs_ingested, expected_min, last_success_at, staleness_seconds, gap_reason, status`
- `pipeline_costs` — `run_id, dag_id, task_id, bytes_scanned, athena_cost_usd, lambda_ms, s3_requests, s3_egress_bytes, run_date`

Iceberg partitioning: bronze by `(source_id, ingest_date)`; `articles` by `days(event_date)`,
where `event_date = coalesce(published_at, fetched_at)` — a deviation from partitioning on
`published_at` directly, recorded in **ADR-0007** (`published_at` is nullable by design, §6.2,
and a null partition key can't be pruned); `hn_comments` by `days(created_at)`; `parse_rejects`
and gold marts small enough to leave unpartitioned. Every table gets a scheduled maintenance
job — compaction, snapshot expiry, orphan file cleanup (§11).

---

## 10. AWS layout and cost discipline

**The clock:** the free plan is $100 credits at signup, up to $100 more from usage, lasting
6 months or until credits deplete — whichever comes first. Therefore **nothing load-bearing may
depend on credits.** The permanent backbone is always-free services only.

**In AWS:** Lambda pollers (1M requests + 400k GB-s/month free; 5 sources × 15 min ≈ 14k/month,
15 sources ≈ 43k/month), S3, Glue Data Catalog (1M objects + 1M requests/month free), Athena,
DynamoDB (25 GB free), CloudWatch (10 metrics, 10 alarms, 5 GB logs), all provisioned by Terraform.

**Local:** Spark, Airflow, Ollama.

**Never:** NAT Gateway (~$32/month to merely exist — keep Lambdas out of VPCs; if a VPC becomes
necessary use the free S3 gateway endpoint), MSK, EMR, Glue ETL jobs and crawlers ($0.44/DPU-hour —
the *catalog* is free, the *compute* is not; register Iceberg tables directly), RDS, OpenSearch,
Redshift.

### 10.1 Egress is the line item nobody budgets

Ingest writes to S3 from inside AWS (free). **Processing reads it from your laptop, which is
internet egress and is billed per GB beyond the free allowance.** Neither superseded document
accounted for this, and it is the one cost that scales with the noisiest source.

- Measure it before enabling GDELT. GDELT 2.0 emits files every 15 minutes; at ~20 MB per interval
  that is ~2 GB/day, ~60 GB/month — inside the 100 GB/month free egress allowance, but not by a
  comfortable margin, and not with room for re-reads.
- **Therefore §6.2's read-once local cache is a cost control, not a convenience.** A re-run that
  re-downloads the window is the failure mode that turns $0 into a real bill.
- Record `s3_egress_bytes` in `ops.pipeline_costs` alongside Athena bytes scanned, and put both in
  the README. **Verified against AWS's own pricing page (2026-08-18): 100 GB/month free data
  transfer out to the internet, aggregated across all services and regions except China and
  GovCloud; $0.09/GB for the next 10 TB after that.** Re-verify before Phase 4A if this paragraph
  is more than a few months old — AWS has changed this allowance before.

### 10.2 Guardrails, before the first line of code

AWS Budgets at $5 and $20, Cost Anomaly Detection, billing alerts by email, `project=signal` tags
on every resource, least-privilege IAM, and stay on the free plan.

### 10.3 Cost as a first-class metric

Athena bills per byte scanned, and fragmented tables also multiply S3 GET requests at $0.40/million
— so compaction becomes a measurable bill reduction rather than housekeeping. Record bytes scanned
and cost per query before and after compaction and put the delta in the README.

### 10.4 Spending the credits deliberately

Budget one time-boxed week to run the gold layer through Redshift Serverless, or a single Glue ETL
job, or a one-shard Kinesis stream — document the cost and the trade-off against the local
equivalent, then `terraform destroy`. "I evaluated X, here is what it cost and why I chose Y" beats
name-dropping X.

**This has a deadline, not a phase.** It was Phase 5 until ADR-0008; sitting in the last phase,
behind the largest one, meant a slip would not delay it but *end* it — the credits expire on the
§10 clock whether or not the phase is reached. It depends on nothing else in the plan, so run it
whenever there is a spare week before the ceiling.

> **Credit expiry: approximately 2027-02-18** — six months from the first `terraform apply`
> (2026-08-18). **Read the exact date off the AWS billing console and replace this line with it.**
> An inferred date is not a deadline, and this is the one item in the plan that cannot be
> recovered by working harder afterwards.

---

## 11. Quality, monitoring, CI

- **Freshness SLA per source**, with dead-feed detection. The common failure is a feed returning 200
  with stale content, not a 500 — detect on content movement, not status code.
- **Volume anomaly detection.** A source dropping 80% overnight should alert, not silently thin the
  brief.
- **Tracked metrics:** dedup ratio, entity-link rate, LLM cache hit rate, schema-failure rate,
  end-to-end latency p50/p95, cost per run, egress per run.
- **Data-quality tests on every model** — uniqueness, referential integrity, accepted values,
  freshness. Hand-written until Phase 5 moves them into dbt.
- **Iceberg maintenance DAG** — daily compaction, snapshot expiry, orphan cleanup, with before/after
  file counts recorded.
- **GitHub Actions** — lint, unit tests, a build against a small fixture warehouse, and the
  §7.1/§7.2/§7.3 eval suites on every PR. **An accuracy regression fails the build.**
- **A compact pipeline-health footer at the bottom of the brief itself**, so anyone who opens the
  output sees the quality layer without reading code.

---

## 12. Build phases

Do not start a phase until the previous one has a working demo and documentation.
Per-phase progress, including what broke on first real use, lives in
[`docs/runbooks/phase-N.md`](docs/runbooks/).

**Carrying an item forward is allowed; losing track of it is not.** Phase 1's 1.D was
deferred by decision and Phase 2 shipped without it — a defensible call, because 1.D gates a
README claim rather than any Phase 2 work. It was carried for two phases and then done, which
is the outcome this rule exists to produce. What makes that safe rather than sloppy is that
it stays visible: anything carried forward is named in the runbook it came from *and* in the
receiving phase's row below, so it is gated by an acceptance test rather than by memory.

| Phase | Deliverable | Acceptance test |
|---|---|---|
| **0. Foundation** *(done)* | Repo, ADRs, §10.2 guardrails before any billable resource; walking skeleton (fake source → brief); CI; eval harness with enforced floors | Fresh clone runs `make setup && make skeleton && make test && make eval` green, CI green, **zero AWS resources beyond guardrails and Terraform state** |
| **1. Ingest** *(done)* | Terraform-provisioned S3 / Glue / DynamoDB / budgets / alarms; 3 Lambda pollers on schedule; `bronze.raw_documents` Iceberg table; local Airflow coordinating monitoring and recovery | Stop ingestion for a day, restart. **Replay** reprocesses the stored interval with no duplicates and no gaps; **catch-up** recovers what each source's backfill horizon allows and records `gap_reason` for the rest (§6.3). **Passed against the deployed pipeline**, 2026-08-21: a deliberate 24.2 h outage, 23,306 rows re-read and 0 committed, and RSS gaps of 0.0 / 3.6 / 5.3 h recorded rather than hidden — `docs/runbooks/phase-1.md` 1.D |
| **2. Lake + query** *(done)* | 6 sources; Spark normalize → `silver.articles`, `silver.hn_comments`, `silver.parse_rejects`; Glue-registered Iceberg; Athena serving queries; partitioning rationale documented (ADR-0007); `ops.pipeline_costs` recording bytes scanned and S3 egress | A stranger runs `make up` and answers an ad-hoc question in Athena; bytes scanned and cost recorded for that query |
| **3. Cluster + resolve** *(done)* | **3.0 first: a real brief** — the existing renderer pointed at real `silver.articles`, ugly ranking, no enrichment, no email. Then: Spark dedup, clustering, entity resolution; both labeled eval sets committed | Reported precision/recall on both, reproducible via `make eval` — **and you have been reading a real brief every morning since 3.0**, not a fake one |
| **4A. Publish** | Ranker over real clusters; HTML brief with §11's health footer, emailed at 07:00; maintenance DAG; **plus the carried-forward items below** | You read it three mornings running and the feedback loop records your marks. Compaction delta measured |
| **4B. Enrich + macro** | Ollama stage with content-hash cache, Pydantic validation and evals (§7.3); ALFRED bitemporal macro store (§8) | A 30-day backfill: **bronze bytes, normalization, hashing, simhash and entity resolution reproduce identically; clustering reproduces within a stated tolerance given a recorded ordering key; enrichment resolves from cache with a published hit rate** |
| **5. Platform polish** | dbt migration of silver→gold; Kafka + Structured Streaming **if and only if §14's criteria are met** | 14+ consecutive daily briefs; each re-added component has a written before/after justification |

Phase 4 is the one people skip and the one interviewers probe. It is not optional — which is
why it is now two phases rather than one ten-item row that can be half-finished and called
done. **ADR-0008** records the split and the reasoning; 4A/4B rather than a renumber, so
existing "Phase 4" references stay valid.

### The brief ladder

The brief appears in Phase 3 and improves monotonically, rather than arriving at the end:

| | Reads from | Ranked by | Delivered |
|---|---|---|---|
| **3.0** | `silver.articles` | recency + breadth (Phase 0's ranker, unchanged) | `make brief` |
| **3.x** | real clusters, resolved entities | same | `make brief` |
| **4A** | same | §7.4's full component set | email, 07:00 |
| **4B** | + enrichment, + macro revisions | same | email, 07:00 |

§1's success criterion is behavioural — *read daily for a month without maintaining it* —
and calendar time is the one input that cannot be compressed by working harder. So the
reading starts as early as something real can be read, which is Phase 3: `brief/ranker.py`,
`brief/render.py` and the Jinja template have all been running since Phase 0, so 3.0 is a
wiring job rather than new code. It also means §7.4's hand-set weights get tuned against
real mornings, and the feedback table has a chance to reach §14's "several hundred marked
items" before the question of automated fitting is worth asking.

### Carried forward into 4A

These are open, recorded in the runbooks, and each gates a claim the README is meant to
make. Listed here because a deliverable that appears in no phase row gets found late.

**1.D has left this table**, which is what the table is for: carried out of Phase 1 by
decision, held here through two phases, and closed on 2026-08-21 against the deployed
pipeline rather than quietly forgotten. Its numbers are in the README.

**Stale-but-successful feed detection has left it too**, and for a less flattering reason:
it was fixed on 2026-08-20 in 1.E and stayed on this list anyway. `State.last_content_change_at`
carries the signal, `assess_source` reports `dead_feed` against a separate per-source SLA, and
the footer prints fetch staleness and content staleness as two columns. A table of open items
that lists a closed one costs the same as one that omits an open one — 4A.A found this by
checking the code against the row rather than trusting either.

| Item | Recorded in | Gates |
|---|---|---|
| **HN score-velocity poller** — a second poller over `topstories.json`; the forward id walk fetches each item once, at score 1, so there is nothing to slope | `docs/runbooks/phase-2.md` | §7.4's velocity component |
| **`project` cost-allocation tag** | `docs/runbooks/phase-1.md` | §10.3's per-project cost answer |
| **Salience vs. resolution** — the brief shows every resolved mention as a subject, so a photo credit puts Getty Images on an Amazon story | `docs/runbooks/phase-3.md` 3.E | §7.4's relevance component |
| **Publisher-diversity inflation** — one HN submission's outbound links count as three publishers | `docs/runbooks/phase-3.md` 3.E | §7.4's breadth component |
| **EDGAR shaping** — one Form 4 clusters twice, once per CIK: the filing is indexed under both the reporting person and the issuer | `docs/runbooks/phase-3.md` 3.E | The brief's top ten |

### Labeled sets are work, and they start now

~600 hand labels gate two phase acceptances: ~200 article pairs (§7.1) and ~300 mentions
(§7.2) for Phase 3, ~100 enrichment examples (§7.3) for 4B.

**Phase 3's two are done**: `evals/dedup/pairs.jsonl` holds 252 real pairs beside the 55
synthetic Phase 0 ones, scored separately so the fixture cannot flatter the real set, and
`evals/entities/mentions.jsonl` holds 300. Both floors are off `0.0` and enforced in CI.
`evals/enrichment/` is still empty, with its floors at `0.0` waiting for 4B.

Label **incrementally against the real `silver.articles` data that already exists**, not in
one block once the code is written — 20 pairs a day through Phase 3's build reaches 200 by
the time the clusterer needs them. Label **before** writing the matching algorithm, so the
labels are not quietly shaped to flatter the implementation they will judge.

Phase 3 confirmed the second rule the expensive way round and added a third. The labeled set
cannot certify a constant it scores flat: of the four the same-story grid searches, 252 pairs
determine **one**, and the rest were being chosen by a tiebreak that had drifted to
recommending a value already measured as harmful. **A constant the labels cannot separate is
decided by a corpus-level measurement** (`evals/experiments/corpus_merge_rate.py`) **or it is
not decided at all.**

---

## 13. Repository layout

```
signal/
├── infra/terraform/        # S3, Glue, DynamoDB, Lambda, EventBridge, budgets, alarms
├── ingest/lambdas/         # one module per source, single shared poll contract
├── spark/jobs/             # normalize, dedup, cluster, resolve
├── enrich/                 # Ollama client, versioned prompts, Pydantic schemas
├── macro/                  # ALFRED vintages, bitemporal load
├── warehouse/              # Iceberg table definitions, Athena SQL  (→ dbt/ in Phase 5)
├── airflow/dags/           # ingest_*, process, enrich, macro, brief, maintenance, quality, backfill
├── brief/                  # ranker, renderer, mailer
├── evals/                  # labeled sets + scorers for dedup, entities, LLM
├── ops/                    # cost tracking, egress accounting, health checks
├── tests/
├── docs/                   # architecture, runbooks, decision log
│   └── archive/            # superseded specs, kept for the decision trail
├── docker-compose.yml
├── Makefile                # make up / make eval / make backfill / make brief / make replay
└── README.md
```

---

## 14. Deferred, with re-entry criteria

Deferral without a re-entry test is just a wish. Each of these returns when its criterion is met,
and not before.

| Deferred | Returns when | Why not now |
|---|---|---|
| **Kafka** | A source arrives that is **genuinely continuous** rather than polled, **and** there is a second, independent consumer of `articles.normalized` that is not the batch clustering job — e.g. a live ticker-alert path with its own latency SLA | **The sources are micro-batch by nature.** Feeds emit on a 5–30 minute cadence and are reached by polling, so there is no continuous upstream stream to preserve — a topic here would manufacture streaming semantics the data never had. It would also sit between one local Spark job and another, with `bronze/` already the replay source of truth. A topic with a single consumer and no process boundary is a résumé line you then have to defend |
| **Spark Structured Streaming** | Kafka returns | Batch on a 15-minute cadence is the same transformation code without the exactly-once argument |
| **dbt** | The gold layer exceeds ~10 models, or hand-written tests start duplicating each other | Below that, dbt's lineage and test framework cost more setup than they save |
| **pgvector** | The vector working set exceeds ~50k embeddings | Concretely: clustering holds a 72-hour window (a few thousand articles) and novelty scores against ~30 days of cluster heads (~1k–3k vectors). That is a numpy array and a cosine call, not a database. Standing up Postgres for 3k vectors is the kind of over-engineering the rest of this document exists to avoid |
| **Automated ranker weight fitting** | The feedback table holds several hundred marked items | See §7.4 |

**Excluded permanently:** Scala, Hadoop, Kubernetes, EMR, MSK, Glue ETL, RDS, OpenSearch, a
dashboard. A dashboard in particular is the fastest way to make this look like every other project
in the pile (§2).

---

## 15. Metrics for the README

Never invent a metric. Add it when the pipeline can calculate and reproduce it.

| Area | Metric |
|---|---|
| Ingestion | documents/day, source success rate, freshness, replay success, catch-up coverage by source |
| Processing | articles in, clusters out, deduplication ratio, p50/p95 latency |
| Quality | clustering precision/recall, entity-resolution precision/recall |
| Local LLM | schema-failure rate, cache-hit rate, eval accuracy by model and prompt version |
| Reliability | consecutive successful daily briefs, failure recovery time |
| Cost | estimated cost/day, Athena bytes scanned, S3 egress, cost before/after compaction |

---

## 16. What goes in the README

1. Architecture diagram and one-command startup.
2. Dedup and entity-resolution precision/recall, with the labeled sets linked.
3. Throughput: articles/day in, clusters/day out, end-to-end latency p50/p95.
4. **Cost:** dollars per day to run, egress, and the compaction before/after delta.
5. A screenshot of a real morning brief, and how many consecutive days it has run.
6. Replay vs catch-up, stated honestly, with the per-source backfill horizons.
7. One section titled "what I'd do differently at 100× volume."
8. One section on a decision you reversed and why. **§0 of this document is the raw material** —
   cutting Kafka is a real reversal with a real reason, which is worth more than a component list.

---

## 17. Non-negotiable rules

Consolidated from both superseded documents; these override convenience everywhere.

- Each source implements `poll(source_config, state) -> (raw_documents, new_state)`.
- Raw source payloads are never overwritten or silently re-fetched.
- Store `fetched_at` separately from source-provided `published_at`.
- Conditional requests use `ETag` and `Last-Modified` where available.
- Every resource has `project=signal` tags and least-privilege IAM.
- Failed records are quarantined with a reason; never silently dropped.
- Every bronze object is downloaded to local at most once, into a content-addressed cache.
- LLM outputs are validated against a typed schema, cached by
  `(input_hash, model_digest, prompt_version)`, with failures retained for inspection.
- Pin the local model digest and version prompts. Track enrichment accuracy on the committed
  labeled set before changing either.
- Keep a per-source freshness SLA and alert on stale-but-successful feeds.
- Record Athena bytes scanned, S3 egress, and estimated cost per query and per run.
- Never claim a metric the pipeline cannot recompute.

---

## 18. Risks

- **The cliché risk.** Everything in §2 exists to counter it. Keep the README ordered accordingly.
- **Scope.** Five sources beats fifteen. Depth in one layer beats breadth across four. §14 exists to
  stop the stack growing back.
- **Feed fragility.** RSS feeds die. §11's dead-feed detection is what keeps the "ran 60 days
  uninterrupted" claim true.
- **The credit clock.** Six months. Re-read §10 before adding any AWS service, and note that
  §10.4's evaluation is the one deliverable with a hard external deadline — it expires rather than
  slips.
- **Back-loading the thing that defines success.** §1's criterion is behavioural and needs calendar
  time nothing else can substitute for. The brief ladder in §12 exists to start that clock in
  Phase 3 rather than at the end of Phase 4; if the ladder gets skipped "to do it properly later,"
  this risk comes straight back.
- **The labeled sets.** ~600 hand labels gate two phase acceptances and are the least interesting
  work in the project, which is exactly why they slip. §12 schedules them incrementally against
  data that already exists; a block of 600 left until the code is ready is a stalled phase.
- **Egress creep.** The one bill that grows quietly with source volume. §10.1.
- **LLM drift.** Pinned digests and the eval suite are what stop a model swap silently degrading the
  brief.
- **Over-claiming reproducibility.** The Phase 4B acceptance test says exactly which stages are
  bit-reproducible and which are not. Keep it that way; the precision is the point — ADR-0008 split
  Phase 4 without touching a word of that test's wording, for exactly this reason.

---

## 19. Definition of done

Signal is ready to show recruiters when a stranger can clone the repository, understand the
architecture in five minutes, deploy or run a small demo safely, inspect real metrics and tests, and
see evidence that the brief has been useful and reliable over multiple weeks.

## 20. Resume statement

Use only once the figures are real:

> Built a production-style data platform that ingests financial and technology news every 15
> minutes, deduplicates syndicated coverage into story clusters, and publishes a daily ranked brief
> with replay, freshness monitoring, per-run cost tracking, and locally run LLM enrichment with
> schema validation and versioned evaluation. Implemented the ingestion and lake layer with
> EventBridge, Lambda, S3, DynamoDB, Glue, and Athena.
