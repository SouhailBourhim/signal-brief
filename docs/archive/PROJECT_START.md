# Signal — Project Start

## Purpose

Build a production-minded data engineering portfolio project that produces a useful daily
technology, markets, and macroeconomic brief. The project should demonstrate strong data
engineering judgment—not merely aggregate headlines—and provide clear, measurable evidence for a
resume and technical interviews. AWS is the initial infrastructure implementation, not the point of
the project.

## One-sentence pitch

Signal ingests public news, filings, and market data every 15 minutes; stores immutable raw data;
turns syndicated articles into ranked story clusters; and delivers a daily brief with data quality,
lineage, replay, cost controls, and locally run LLM enrichment built in.

## What makes it interview-worthy

The project will be judged by evidence, not the number of services in its architecture. Its
differentiators are:

- A scheduled, idempotent data-lake pipeline with infrastructure as code.
- Immutable raw data and a demonstrated replay/backfill path.
- Story-level deduplication rather than a generic news feed.
- Measured entity-resolution and clustering quality using committed labeled examples.
- A local LLM enrichment stage with versioned prompts, a content-addressed cache, typed outputs,
  and an evaluation set—so model output is governed like any other data transform.
- Explicit freshness, volume, cost, and pipeline-health monitoring.
- A real daily output that has run reliably over time.

## Initial architecture

```text
Airflow (local DAG orchestration)
        |
        +──> EventBridge Scheduler ──> Lambda pollers ──> S3 bronze (raw, immutable)
        |                                      |                     |
        |                                      v                     v
        |                               DynamoDB state        Glue Catalog / Iceberg
        |                                                            |
        +──> Kafka: articles.normalized ──> Spark processing ──> Local LLM enrichment + validation
                                                              |                     |
                                                              v                     v
                                                        Athena queries       S3 silver and gold data
                                                                    |
                                                                    v
                                                   Ranked HTML brief + health footer
```

The initial cloud implementation uses S3, Lambda, EventBridge Scheduler, DynamoDB, Glue Data
Catalog, Athena, CloudWatch, IAM, AWS Budgets, and Terraform. Kafka is the local streaming
platform for normalized article events (Redpanda is an acceptable local implementation). Those
choices support the project; they are not the project's headline.

Run Airflow, Kafka, Spark, Postgres/pgvector, and local LLM inference locally at first. Airflow
owns the end-to-end DAGs: processing, enrichment, quality checks, publishing, replay/backfill, and
table maintenance. Kafka carries normalized article events into processing. This keeps costs
predictable while preserving a credible cloud data-platform boundary.

## MVP: what to build first

The first version should be useful by itself and intentionally avoid premature platform complexity.

### Inputs

- One technology RSS feed
- Hacker News API
- SEC EDGAR current filings feed

### Required behaviour

1. Poll each source on a schedule and save the exact raw response plus fetch metadata to S3.
2. Persist watermarks, ETags, and seen identifiers in DynamoDB so retries are safe.
3. Normalize items into an Iceberg or Parquet-backed `articles` dataset catalogued in Glue and
   publish normalized-article events to Kafka.
4. Consume Kafka events in the processing stage, then query the curated dataset in Athena and
   produce a static HTML brief.
5. Detect duplicate URLs and exact-content duplicates before publication.
6. Enrich selected story clusters with a local LLM, producing a validated one-sentence summary and
   topic classification.
7. Show source freshness, article count, failures, and estimated query cost in the brief footer.
8. Provision all cloud resources with Terraform and document a clean deploy/destroy path.

### MVP acceptance test

Stop ingestion for one day, then restart it. The system must ingest the missed interval without
duplicates, and the resulting normalized dataset must be reproducible from the raw S3 objects.

## Phased roadmap

| Phase | Outcome | Evidence to publish |
|---|---|---|
| 1. Ingestion foundation | Scheduled pollers land immutable raw data; Airflow coordinates monitoring and recovery | Architecture diagram, infrastructure code, storage layout, replay demonstration |
| 2. Streaming lake and query | Normalized articles flow through Kafka; Glue catalog and Athena query curated data | Topic contract, consumer behaviour, partitioning rationale, query cost |
| 3. Story intelligence | Spark processing consumes article events for near-duplicate detection, clusters, and ticker/entity linking | Labeled evaluation set; precision, recall, and dedup-ratio results |
| 4. Enrich and publish | Local LLM produces validated cluster summaries; ranked HTML brief runs at 07:00 with feedback capture | Eval results, cache-hit rate, screenshots, 14+ consecutive daily runs, latency and freshness metrics |
| 5. Production polish | CI, data-quality tests, Iceberg maintenance, and bitemporal macro data | Failure/replay walkthrough, compaction delta, schema-evolution example |

Do not start the next phase until the previous phase has a working demo and documentation.

## Data contracts and non-negotiable rules

- Each source implements `poll(source_config, state) -> (raw_documents, new_state)`.
- Raw source payloads are never overwritten or silently re-fetched.
- Store `fetched_at` separately from source-provided `published_at`.
- Conditional requests use `ETag` and `Last-Modified` where available.
- Every resource has `project=signal` tags and least-privilege IAM.
- Failed records are quarantined with a reason; do not silently drop them.
- Local LLM outputs must be validated against a typed schema. Cache results by input hash, model
  digest, and prompt version; retain failures for inspection rather than retrying indefinitely.
- Pin the local model digest and version prompts. Track enrichment accuracy on a committed labeled
  evaluation set before changing either.
- Keep a per-source freshness SLA and alert on stale-but-successful feeds.
- Record Athena bytes scanned and estimated cost per query/run.

## Metrics that belong in the README

| Area | Metric |
|---|---|
| Ingestion | documents/day, source success rate, freshness, replay success |
| Processing | articles in, clusters out, deduplication ratio, p50/p95 latency |
| Quality | clustering precision/recall, entity-resolution precision/recall |
| Local LLM | schema-failure rate, cache-hit rate, evaluation accuracy by model and prompt version |
| Reliability | consecutive successful daily briefs, failure recovery time |
| Cost | estimated cost/day, Athena bytes scanned, cost before/after compaction |

Never invent a metric. Add it when the pipeline can calculate and reproduce it.

## Suggested repository shape

```text
signal/
├── infra/terraform/       # AWS resources, IAM, budgets, alarms
├── ingest/lambdas/        # source adapters and shared poll contract
├── streaming/             # Kafka topic contracts, producers, and consumers
├── processing/            # normalization, deduplication, clustering, entities
├── warehouse/             # Iceberg definitions and Athena SQL
├── airflow/dags/          # process, enrich, quality, brief, backfill, maintenance DAGs
├── brief/                 # ranking, HTML renderer, delivery
├── evals/                 # committed labeled test data and scorers
├── ops/                   # health checks and cost reporting
├── tests/
├── docs/                  # architecture and operational runbooks
├── Makefile
└── README.md
```

## Initial implementation checklist

- [ ] Create the repository and Python project scaffold.
- [ ] Configure Terraform state and an AWS region.
- [ ] Add AWS Budgets alerts before creating billable resources.
- [ ] Provision S3, DynamoDB, IAM roles, Lambda, EventBridge schedules, and CloudWatch logs.
- [ ] Implement one RSS poller with conditional requests and raw S3 persistence.
- [ ] Add a second source to prove the adapter contract.
- [ ] Run Airflow and Kafka locally with Docker Compose; create a DAG that coordinates ingestion,
  processing, validation, and reporting.
- [ ] Publish normalized articles to a documented Kafka topic and consume them in processing.
- [ ] Catalog and query curated data using Glue and Athena.
- [ ] Write a replay test using stored raw data.
- [ ] Run a local model (for example, through Ollama) to summarize clusters into a typed schema.
- [ ] Add prompt/model versioning, caching, and a small labeled enrichment evaluation set.
- [ ] Publish the first static daily brief.
- [ ] Add CI for formatting, unit tests, and infrastructure validation.

## Explicitly deferred

- Spark Structured Streaming
- pgvector
- dbt
- ALFRED bitemporal macro modelling
- A dashboard

These are later enhancements, not proof that the core pipeline works.

## Resume-ready outcome

Only use a statement like this once the figures are real:

> Built a production-style data platform that ingests financial and technology news every 15
> minutes, deduplicates syndicated coverage into story clusters, and publishes a daily ranked brief
> with replay, freshness monitoring, per-run cost tracking, and locally run LLM enrichment with
> schema validation and versioned evaluation. Implemented the ingestion and lake layer with
> EventBridge, Lambda, S3, DynamoDB, Glue, and Athena.

## Definition of done

Signal is ready to show recruiters when a stranger can clone the repository, understand the
architecture in five minutes, deploy or run a small demo safely, inspect real metrics and tests,
and see evidence that the brief has been useful and reliable over multiple weeks.
