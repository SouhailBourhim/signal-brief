# Signal — Design Specification

> **Amended 2026-08-24 — the brief now sends at 16:00, not 07:00.** The times recorded
> below are what the schedule was at the time and are left as written. The send moved
> because the host sleeps through the small hours: on 2026-08-24 the scheduler logged
> nothing between 21:00 and 12:58 UTC, then resumed mid-stride and fired the whole chain
> at once, so the brief landed at 13:59. The containers never died — they were frozen
> with the host, which still reported them `Up`. See `airflow/dags/brief_dag.py`.

*A daily tech / finance / economy brief, and the pipeline that earns it.*

Version 2 — consolidated. Supersedes `DESIGN.md`, which recorded how these decisions were reached.
This document states the decisions and is meant to be built from directly.

---

## 1. What it is

Every 15 minutes, ingest ~15 free news, filing, and macro feeds. Collapse thousands of articles
into a few dozen **story clusters**. Resolve the companies mentioned to real tickers. Rank clusters
by novelty, breadth, and whether the market actually reacted. Publish a brief at **07:00
Africa/Casablanca** that you read every morning and mark up, which trains the ranker.

**Success is behavioural, not technical:** the project works when you read it daily for a month
without maintaining it. Everything below exists to make that true.

## 2. Why it isn't a news aggregator

The aggregation is roughly 200 lines. The project is the four layers around it:

1. **Story-level deduplication** — the same acquisition arrives as 40 articles.
2. **Entity resolution** — mentions to canonical companies to tickers, with measured accuracy.
3. **A local LLM as a governed pipeline stage** — cached, validated, evaluated, versioned.
4. **A bitemporal macro store** — because CPI and payrolls get revised for months.

"News aggregator with sentiment analysis" is the most-built project in this space. The README must
lead with those four, with numbers attached. Lead with a dashboard screenshot and it disappears
into the pile.

---

## 3. Sources

| Source | Gives | Cadence | The interesting difficulty |
|---|---|---|---|
| [GDELT 2.0](https://www.gdeltproject.org/) | Global news stream, tone + entity annotations | 15 min | High volume, noisy, multilingual, files occasionally late or empty |
| RSS: TechCrunch, The Verge, Ars, Reuters/AP tech | Curated tech coverage | 5–30 min | `pubDate` often wrong or absent; feeds go stale while still returning 200 |
| [Hacker News API](https://github.com/HackerNews/API) | Community attention, score velocity | 1 min | Score is time-dependent — snapshot it, never overwrite |
| [SEC EDGAR RSS](https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent) | 8-K, S-1, Form D as filed | near real time | Requires descriptive `User-Agent` with contact email; fair-access rate limits |
| SEC **Form D** | Startup fundraises, often ahead of press | daily | Underused; a genuine scoop source |
| [FRED / ALFRED](https://fred.stlouisfed.org/docs/api/fred/) | Macro series **with vintages** | per release | Revisions are the point — see §8 |
| ECB / Fed / BoE press RSS | Central bank statements | irregular | Long docs; value is in the diff against the previous statement |
| Stooq / yfinance | Prices, indices, FX | daily | Splits and dividends retroactively rewrite history |
| arXiv (cs.LG, econ) | Research signal | daily | v1→v2 are updates, not new papers |

Start with five. Adding source #11 must be a 30-minute job — that constraint drives the ingestion
design in §5.

---

## 4. Architecture

```
                        ┌─────────────── AWS (always-free tier) ───────────────┐
                        │                                                       │
  feeds ──────────────► │  EventBridge Scheduler ──► Lambda poller (1 per src)  │
                        │            │                                          │
                        │            ├──► S3  bronze/  (raw, immutable)         │
                        │            └──► DynamoDB  (etags, watermarks, seen)   │
                        └───────────────────────┬───────────────────────────────┘
                                                │
                        ┌───────────────── local (docker compose) ──────────────┐
                        │                       ▼                                │
                        │   Spark: normalize ──► Kafka: articles.normalized      │
                        │                       │                                │
                        │   Spark Structured Streaming:                          │
                        │     simhash/LSH ─► embed ─► incremental cluster        │
                        │                       │                                │
                        │              entity resolution (dict + context)        │
                        │                       │                                │
                        │              Ollama enrichment (cached, validated)     │
                        │                       │                                │
                        │   pgvector ◄──────────┤                                │
                        │                       ▼                                │
                        │      Iceberg tables on S3, Glue Data Catalog           │
                        │                       │                                │
                        │              dbt (silver ─► gold marts)                │
                        │                       │                                │
                        │              ranker ──► brief renderer                 │
                        └───────────────────────┬────────────────────────────────┘
                                                ▼
                             static HTML + email, 07:00 Africa/Casablanca
                                                │
                                     feedback ──┘ (trains ranker)

  Athena queries Iceberg-on-Glue directly for ad-hoc work and the serving layer.
  Airflow (local) orchestrates everything, including the AWS-side resources.
```

## 5. Stack

| Layer | Choice | Rationale |
|---|---|---|
| Orchestration | **Airflow 3** | 29% of postings; Dagster's asset model is nicer but barely appears in job ads |
| Compute | **PySpark** — batch + Structured Streaming | 33% of postings, largest single salary premium (~+$20K) |
| Streaming | **Kafka** (Redpanda locally) | 17%; Kafka-protocol compatible, a fraction of the resource cost |
| Table format | **Apache Iceberg** on S3 | 58% of surveyed enterprises use it for business-critical workloads; 79% migrating more within a year |
| Catalog | **AWS Glue Data Catalog** | First 1M objects + 1M requests/month free; the standard AWS Iceberg catalog |
| Ingestion | **Lambda + EventBridge Scheduler** | Always free at this volume; genuinely serverless |
| Pipeline state | **DynamoDB** | 25 GB always free; right shape for watermarks and seen-sets |
| Query / serving | **Athena** | $5/TB scanned, 10 MB minimum — cents monthly here, zero infrastructure |
| Modeling | **dbt** | 24%, +$11K premium |
| Vectors | **Postgres + pgvector** | LLM/GenAI postings ask specifically for retrieval pipelines and vector stores |
| Inference | **Ollama** (8B-class, pinned digest) | Zero marginal cost, and forces real thinking about determinism |
| IaC | **Terraform** | 14%; also what stops the project smelling console-driven |
| CI | **GitHub Actions** | CI/CD appears in 30% of postings and costs an afternoon |

Deliberately excluded: Scala, Hadoop, Kubernetes, EMR, MSK, Glue ETL jobs. Reasons in §10.

---

## 6. Ingestion

**One Lambda per source**, all conforming to a single contract, because adding source #11 must be
trivial:

```python
def poll(source_config, state) -> tuple[list[RawDocument], NewState]
```

Rules:

- **Raw payloads are immutable and never re-fetched.** Everything downstream is recomputable from
  `bronze/`. This is what makes a 30-day backfill possible in phase 4.
- **Conditional requests** — persist `ETag` / `Last-Modified` in DynamoDB, send `If-None-Match`.
  Most feeds return 304 most of the time; this is the difference between polite and rate-limited.
- **Fetch metadata is data** — status code, latency, byte count, and `fetched_at` are stored
  alongside the payload. §11's monitoring is built entirely from these.
- **Trust no timestamp.** Record `fetched_at` (certain) separately from `published_at` (claimed).
  Where they disagree by more than a threshold, flag rather than silently correct.
- **Per-source rate limiting and a descriptive `User-Agent`.** SEC requires a contact email in it;
  getting blocked by EDGAR mid-project is an avoidable afternoon.

### Bronze layout
```
s3://signal-bronze/source={source_id}/ingest_date={yyyy-mm-dd}/hour={hh}/*.parquet
```
Iceberg table `bronze.raw_documents`, partitioned by `(source_id, ingest_date)`.

---

## 7. Processing layers

### 7.1 Deduplication and story clustering
Four stages, cheapest first:

1. **Exact** — content hash after boilerplate stripping.
2. **Near-duplicate** — 64-bit simhash with banded LSH. Catches syndication and light rewrites.
3. **Same-story clustering** — sentence-transformer embeddings, incremental clustering over a
   time-decayed 72-hour window. "Apple acquires X" and "X to be bought by Apple" are one event.
4. **Canonical selection** — earliest credible publisher becomes cluster head; the remainder
   become `distinct_publisher_count`, which feeds ranking rather than being discarded.

**Publish:** dedup ratio (articles in ÷ clusters out) and precision/recall against ~200 hand-labeled
article pairs. The labeled set lives in `evals/` and is committed.

### 7.2 Entity resolution
Dictionary built from SEC `company_tickers.json` plus Wikidata aliases. Then the hard cases:
"Meta" versus metadata, Apple Inc. versus an apple supplier, subsidiaries rolling to parents,
private companies with no ticker, and **companies that rename** — which is an SCD2 problem wearing
a disguise, handled in `dim.entities` with `valid_from` / `valid_to`.

Disambiguation by cosine similarity between article context and entity description embeddings,
with a confidence floor below which a mention is left unlinked rather than guessed.

**Publish:** precision/recall over 300 hand-labeled mentions. Very few portfolio projects quantify
this, which is exactly why it reads as senior.

### 7.3 LLM enrichment (Ollama)
Three jobs per cluster: a one-sentence summary, topic classification, and structured extraction
(company, amount, round type, headcount delta, filing type) into a typed schema.

Governed like any other transform:

- **Content-hash cache** keyed on `(input_hash, model_digest, prompt_version)` — never re-infer.
  Cache hit rate is a published metric.
- **Pydantic validation** on every output. Failures are quarantined to `gold.enrichment_rejects`,
  never silently dropped.
- **Eval harness** — 100 labeled examples in `evals/`, accuracy tracked per model version, so
  swapping models is a measurement rather than a vibe.
- **Determinism boundary** — temperature 0, pinned model digest, versioned prompts, and everything
  downstream marked non-reproducible in lineage. Being explicit about this is the mature move.
- **Backpressure** — a 4-second inference inside a 15-minute cycle is a capacity calculation, and
  the DAG should fail loudly rather than silently lag.

### 7.4 Ranking
A brief is useful because of what it **omits**. Score each cluster:

| Component | Signal |
|---|---|
| Novelty | Embedding distance to the last 30 days of clusters — recycled narratives sink |
| Breadth | Count of *independent* publishers (§7.1 makes this honest) |
| Velocity | HN score slope, mention-rate acceleration |
| Relevance | Your watchlist of companies, technologies, macro series |
| Market corroboration | Did the linked ticker or rate move beyond its normal range? |
| Feedback | Your morning thumbs up/down, fed back as training signal |

Weights start hand-set and are refit weekly once feedback accumulates. Store
`score_components` as a map so every ranking decision is explainable after the fact.

---

## 8. The bitemporal macro store

Macro data is revised for months after first publication. A normal pipeline overwrites and quietly
destroys the record; this one keeps two time axes:

- `valid_time` — the period the number describes
- `known_time` — the vintage date it was published

so "what was knowable on 2026-03-14" is a query, not an archaeology project.

Two payoffs. The brief can state **"payrolls revised down 46k across the prior two months"** — a
thing coverage routinely buries and which often matters more than the headline print. And
bitemporal modeling against genuinely revising data is a senior warehouse skill that arrives here
naturally instead of being bolted on for show.

---

## 9. Data model

**Bronze**
- `raw_documents` — `ingest_id, source_id, fetched_at, source_url, http_status, etag, content_hash, payload, payload_format`

**Silver**
- `articles` — `article_id, source_id, url_canonical, title, body_text, published_at, fetched_at, lang, publisher_domain, authority_score, simhash, content_hash`
- `story_clusters` — `cluster_id, first_seen_at, last_updated_at, canonical_article_id, article_count, distinct_publisher_count`
- `article_cluster_map` — `article_id, cluster_id, similarity, assigned_at`
- `entity_mentions` — `mention_id, article_id, surface_form, char_span, entity_id, confidence, resolution_method`
- `dim_entities` (SCD2) — `entity_id, canonical_name, ticker, cik, entity_type, valid_from, valid_to, is_current`

**Gold**
- `cluster_enrichment` — `cluster_id, model_name, model_digest, prompt_version, summary, topic, extracted_json, generated_at, input_hash, cache_hit`
- `macro_observations` (bitemporal) — `series_id, period, value, vintage_date, is_latest, revision_delta`
- `brief_items` — `brief_date, rank, cluster_id, score, score_components, included, user_feedback`

**Ops**
- `source_health` — `source_id, window_start, docs_ingested, expected_min, last_success_at, staleness_seconds, status`
- `pipeline_costs` — `run_id, dag_id, task_id, bytes_scanned, athena_cost_usd, lambda_ms, s3_requests, run_date`

Iceberg partitioning: bronze by `(source_id, ingest_date)`; `articles` by `days(published_at)`;
gold marts small enough to leave unpartitioned. Every table gets a scheduled maintenance job —
compaction, snapshot expiry, orphan file cleanup (§11).

---

## 10. AWS layout and cost discipline

**The clock:** the free plan is $100 credits at signup, up to $100 more from usage, lasting
6 months or until credits deplete — whichever comes first. Therefore **nothing load-bearing may
depend on credits.** The permanent backbone is always-free services only.

**In AWS:** Lambda pollers (1M requests + 400k GB-s/month free; 15 sources × 15 min ≈ 43k/month),
S3, Glue Data Catalog (1M objects + 1M requests/month free), Athena, DynamoDB (25 GB free),
CloudWatch (10 metrics, 10 alarms, 5 GB logs), all provisioned by Terraform.

**Local:** Spark (EMR buys nothing — `s3a` is the same code), Kafka (MSK is the most expensive
thing attachable to this project), Airflow (MWAA is hundreds monthly), Ollama, Postgres/pgvector.

**Never:** NAT Gateway (~$32/month to merely exist — keep Lambdas out of VPCs; if a VPC becomes
necessary use the free S3 gateway endpoint), MSK, EMR, Glue ETL jobs and crawlers ($0.44/DPU-hour —
the *catalog* is free, the *compute* is not; register Iceberg tables directly), RDS, OpenSearch,
Redshift.

**Guardrails, before the first line of code:** AWS Budgets at $5 and $20, Cost Anomaly Detection,
billing alerts by email, `project=signal` tags on every resource, and stay on the free plan.

**Cost as a first-class metric.** Athena bills per byte scanned, and fragmented tables also
multiply S3 GET requests at $0.40/million — so compaction becomes a measurable bill reduction
rather than housekeeping. Record bytes scanned and cost per query before and after compaction and
put the delta in the README.

**Spending the credits deliberately.** Budget one time-boxed week to run the gold layer through
Redshift Serverless, or a single Glue ETL job, or a one-shard Kinesis stream — document the cost
and the trade-off against the local equivalent, then `terraform destroy`. "I evaluated X, here is
what it cost and why I chose Y" beats name-dropping X.

---

## 11. Quality, monitoring, CI

- **Freshness SLA per source**, with dead-feed detection. The common failure is a feed returning
  200 with stale content, not a 500 — detect on content movement, not status code.
- **Volume anomaly detection.** GDELT dropping 80% overnight should alert, not silently thin the brief.
- **Tracked metrics:** dedup ratio, entity-link rate, LLM cache hit rate, schema-failure rate,
  end-to-end latency p50/p95, cost per run.
- **dbt tests** on every model — uniqueness, referential integrity, accepted values, freshness.
- **Iceberg maintenance DAG** — daily compaction, snapshot expiry, orphan cleanup, with
  before/after file counts recorded.
- **GitHub Actions** — lint, unit tests, dbt build against a small fixture warehouse, and the
  §7.1/§7.2/§7.3 eval suites on every PR. An accuracy regression fails the build.
- **A compact pipeline-health footer at the bottom of the brief itself**, so anyone who opens the
  output sees the quality layer without reading code.

---

## 12. Build phases

| Phase | Deliverable | Acceptance test |
|---|---|---|
| **1. Ingest** | Terraform-provisioned S3/Glue/DynamoDB/budgets; 5 Lambda pollers on schedule; bronze Iceberg table | Kill the pipeline for a day, restart, and replay that day from raw with no gaps |
| **2. Cluster + resolve** | Spark dedup, clustering, entity resolution; labeled eval sets committed | Reported precision/recall on both, reproducible via `make eval` |
| **3. Enrich + publish** | Ollama stage with cache and evals; ranker; HTML brief emailed at 07:00 | You read it three mornings running and the feedback loop records your marks |
| **4. Correctness + ops** | ALFRED bitemporal macro, quality marts, maintenance DAG, CI, cost tracking | A 30-day backfill reproduces byte-identical output; compaction delta measured |

Phase 4 is the one people skip and the one interviewers probe. It is not optional.

---

## 13. Repository layout

```
signal/
├── infra/terraform/        # S3, Glue, DynamoDB, Lambda, EventBridge, budgets, alarms
├── ingest/lambdas/         # one module per source, single shared contract
├── spark/jobs/             # normalize, dedup, cluster, resolve
├── enrich/                 # Ollama client, prompts (versioned), schemas
├── dbt/                    # silver + gold models, tests, docs
├── airflow/dags/           # ingest_*, process, enrich, macro, brief, maintenance, quality
├── brief/                  # ranker, renderer, mailer
├── evals/                  # labeled sets + scoring for dedup, entities, LLM
├── ops/                    # cost tracking, health checks
├── tests/
├── docker-compose.yml
├── Makefile                # make up / make eval / make backfill / make brief
└── README.md
```

---

## 14. What goes in the README

1. Architecture diagram and one-command startup.
2. Dedup and entity-resolution precision/recall, with the labeled sets linked.
3. Throughput: articles/day in, clusters/day out, end-to-end latency p50/p95.
4. **Cost:** dollars per day to run, and the compaction before/after delta.
5. A screenshot of a real morning brief, and how many consecutive days it has run.
6. One section titled "what I'd do differently at 100× volume."
7. One section on a decision you reversed and why.

## 15. Risks

- **The cliché risk.** Everything in §2 exists to counter it. Keep the README ordered accordingly.
- **Scope.** Five sources beats fifteen. Depth in one layer beats breadth across four.
- **Feed fragility.** RSS feeds die. §11's dead-feed detection is what keeps the "ran 60 days
  uninterrupted" claim true.
- **The credit clock.** Six months. Re-read §10 before adding any AWS service.
- **LLM drift.** Pinned digests and the eval suite are what stop a model swap silently degrading
  the brief.
