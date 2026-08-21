# Signal

*A daily tech / finance / economy brief, and the pipeline that earns it.*

Ingests public news, filings, and macro data on a 15-minute cadence; stores raw responses
immutably; collapses syndicated coverage into ranked story clusters; and publishes a brief
at 07:00 Africa/Casablanca with lineage, replay, cost controls, and locally run LLM
enrichment built in.

**Status: Phase 3, done bar the write-up.** Six pollers — Hacker News, SEC EDGAR + Form D,
and three RSS/Atom feeds — run as scheduled Lambdas and land raw payloads in S3; local Spark
jobs commit them to `bronze.raw_documents` on Iceberg, normalize that into `silver.articles`
and `silver.hn_comments`, collapse it into story clusters, and resolve company mentions
against a pinned SEC + Wikidata dictionary. **Both labeled eval sets are committed and
scored** — 252 article pairs and 300 entity mentions — and a real brief has been read every
morning since 3.0, which is what Phase 3's acceptance actually asks for. The infrastructure
is Terraform, and applied. No LLM yet.

What remains in the phase is 3.E: ratcheting the accuracy floors and writing ADR-0009, which
has two verdicts to record — whether sentence embeddings beat the lexical same-story rule,
and whether they beat the lexical resolver on the `Meta`-versus-metadata class it cannot
touch. [`docs/runbooks/phase-3.md`](docs/runbooks/phase-3.md) is where this phase stands,
including what broke on first real use, and there is a lot of that.

**Replay and catch-up are not the same promise.** Replay reprocesses an interval from bytes
already in `bronze/` — deterministic, and always available. Catch-up re-fetches what was
missed during downtime, and is bounded by each source's backfill horizon: Hacker News can be
recovered completely, EDGAR for about a day, and an RSS feed only as far back as its current
window. What catch-up cannot recover is recorded as a `gap_reason` per source per interval
in `ops.source_health` and printed in the brief's footer, rather than left to look like a
quiet day (SPEC §6.3).

**The shape of it**, in one line: ingestion is serverless and in AWS because it must run
whether or not a laptop is on; processing is local because EMR, MSK and MWAA buy nothing here
that `s3a` and Docker Compose do not. [`docs/architecture.md`](docs/architecture.md) draws
both halves, the table lineage, and what is deliberately still missing.

**New to data engineering?** [`docs/how-signal-works.md`](docs/how-signal-works.md) explains
what each phase is for, in plain English, with no prior knowledge assumed.

## Quickstart

```bash
make setup      # uv sync + pre-commit hooks
make skeleton   # fake source -> bronze -> silver -> clusters -> HTML brief
make test
make eval       # score labeled sets, enforce accuracy floors
```

`make skeleton` writes `out/brief-<date>.html`. It touches no network and no AWS account.

Requires Python 3.12, JDK 17 (Spark 4), Docker, and — for Phases 1+ — Terraform ≥ 1.11 and
an AWS account with the guardrails in SPEC §10.2 already in place. On Windows, run
everything inside WSL2 (ADR-0002).

## What this is not

"News aggregator with sentiment analysis" is the most-built project in this space. The
aggregation here is a couple of hundred lines. The project is the four layers around it:

1. **Story-level deduplication** — the same acquisition arrives as 40 articles.
2. **Entity resolution** — mentions to canonical companies to tickers, with measured accuracy.
3. **A local LLM as a governed pipeline stage** — cached, validated, evaluated, versioned.
4. **A bitemporal macro store** — because CPI and payrolls get revised for months.

## Measured, not claimed

SPEC §15: never publish a metric the pipeline cannot recompute. Current numbers:

| Metric | Value | Source |
|---|---|---|
| Dedup precision / recall, **real pairs** | **1.000 / 0.500** held out · 0.962 / 0.568 full set (n=252) | `evals/fit_thresholds.py`, labeled 2026-08-20 |
| Dedup precision / recall, Phase 0 fixture | 1.000 / 1.000 (n=55) | `make eval` — a harness canary, not evidence |
| **Entity resolution precision / recall** | **0.833 / 0.556** held out · 0.868 / 0.611 full set (n=300) | `evals/fit_thresholds.py --set entities`, labeled 2026-08-20 |
| Dedup ratio | 11 → 7 clusters (fake) · 4,301 → 4,253 (real; 21 multi-publisher stories) | `make skeleton` / `signal brief` |
| Entity resolution, one production window | 20,760 mentions detected over 4,303 articles → 2,509 linked (12.1%), 1,018 distinct companies | `spark/jobs/resolve.py`, real AWS |
| Cost of one brief | 3 Athena queries, 1.7 MB scanned, **$0.00014** | brief footer, real AWS |
| Ingestion, one production window | 521 bronze rows → 207 articles (19 quarantined, all `hackernews`/dead-item) | `docs/runbooks/phase-2.md` 2.E, real AWS |
| Athena, `SELECT *` vs. projected vs. partition-pruned, same question | 184,259 / 73,373 / 64,713 bytes scanned | `docs/athena.md`, real AWS |
| S3 egress, one commit | 3,468,248 bytes | `ops.pipeline_costs`, real AWS |
| LLM eval accuracy, cache-hit rate | — | Phase 4B |
| Cost per day (full pipeline) | — | Phase 4A — pieces above are real, a full day's total isn't assembled yet |
| Consecutive daily briefs read | 2 | SPEC §12's brief ladder; the count started 2026-08-20 |

The fixture's 1.000/1.000 proved the harness runs, not that the clustering is good — and
Phase 3's 252 real labeled pairs showed how much daylight sat between those two claims. On a
base-rate sample the Phase 0 rule made 34 merges and **every one was wrong**, while missing
23 of 43 genuine same-story pairs; one cluster in the first real brief swallowed 64% of the
corpus. 3.B fixed both, and the numbers above are what replaced them. The bad ones stayed
published while they were bad — a metric you report only once it flatters you is not a
metric — and the whole arc is in [`docs/runbooks/phase-3.md`](docs/runbooks/phase-3.md).

**Entity resolution reached 0.833 / 0.556 the same way, and the interesting result was that
more data made it worse.** Adding every business Wikidata knows dropped held-out precision
below the SEC-only baseline — an alias index is only as precise as its rarest junk entry, and
the subclass closure of "business" contains every football club ever recorded. A stricter
notability floor made the dictionary a third of the size *and* better on both axes. Two
fitting procedures were also tried and thrown away because the held-out half caught them
overfitting; the one that survived is in `evals/fit_thresholds.py` with the rejected ones
documented beside it.

**Reading the brief is what finds the defects.** 3.D pointed it at the real cluster and entity
tables and, with every test and both eval gates green, the page was wrong four ways: a
deployed table two columns behind its DDL, a staleness warning that fired every day, a
45-article cluster merging a Disney lawsuit with a corgi tracker, and a `breadth` floor that
put nine SEC form numbers on the front page. None of them had a failing test. That is the
argument for SPEC §12's brief ladder, and it is why the count of briefs read is in the table
above.

Three caveats carried on purpose. **Quote the held-out row, not the full set**: half of each
full set was fitted on, so 0.962 and 0.868 are optimistic by construction, while 1.000 / 0.500
and 0.833 / 0.556 were measured on examples the fitting never saw. **Parts of the resolver are
inert at the fitted floor** — prefix matching and context corroboration both locate an entity
and then decline to link it — and that is documented and pinned in tests rather than left to
imply the system does more than it does. And **these labels were made by an LLM assistant and
then reviewed by the reader**, who overrode three (`labeler` is stamped on every record,
`reviewed_from` on every overridden one) — so the figures measure agreement with a model on
the bulk of each set, spot-checked where the rule and the labeler disagreed.

Every Athena dollar figure behind the bytes-scanned numbers above floors at Athena's real 10 MB
per-query minimum (`ops/athena.py`) and currently rounds to the same $0.0000477 per query —
the lake is still small enough that bytes scanned, not cost, is the metric that actually
moves; see `docs/athena.md` for why that's stated rather than hidden.

## Layout

| Path | Contents |
|---|---|
| [`SPEC.md`](SPEC.md) | The specification. Start here |
| [`src/signal_core/contracts.py`](src/signal_core/contracts.py) | The poll contract every source implements |
| `src/signal_core/` | Sources, transform, dedup, entities, Spark jobs, ranking, rendering, ops |
| `warehouse/entities/` | The pinned SEC + Wikidata dictionary the resolver is measured against |
| `handlers/` | Lambda entry point — one artifact, N functions |
| `infra/terraform/` | `bootstrap/` (state backend), `main/` (everything else) |
| `evals/` | Labeled sets, scorers, and the accuracy floors CI enforces |
| [`docs/architecture.md`](docs/architecture.md) | What runs where, and why the AWS/local line falls where it does — diagrams, table lineage, and what is not built yet |
| [`docs/athena.md`](docs/athena.md) | Querying the lake: setup, real questions, the `SELECT *` vs. projected vs. partition-pruned measurement |
| [`docs/how-signal-works.md`](docs/how-signal-works.md) | What each phase is for, in plain English — no prior knowledge assumed |
| [`docs/decisions/`](docs/decisions/) | ADRs, including the ones that reversed earlier choices |
| `docs/archive/` | Superseded specs, kept for the decision trail |

## Adding a source

The design's central claim is that this takes 30 minutes:

1. Write `src/signal_core/sources/<name>.py` implementing
   `poll(config, state) -> (list[RawDocument], State)`.
2. Register it in `src/signal_core/sources/__init__.py` and `config.SOURCES` — declaring
   its **backfill horizon**, which determines what catch-up can honestly promise (SPEC §6.3).
3. Write `src/signal_core/parse/<name>.py` — usually a one-line binding of
   `feedparse.parse_feed` for RSS/Atom — and register it in `parse/__init__.py`. Without
   this, the source polls and commits to bronze fine and then fails silently on the
   silver side the first time `normalize_window` runs.
4. Add one entry to the Terraform `sources` map.

`tests/test_source_registry.py` asserts all four places agree, so a missed step fails a
test rather than a Lambda at 3am.

## Replay and catch-up are different

- **Replay** — reprocess an interval from bytes already in `bronze/`. Always possible and
  deterministic for every stage except clustering and LLM enrichment, whose exact
  guarantees are stated in SPEC §12.
- **Catch-up** — re-fetch what was missed during downtime. Bounded by each source's
  backfill horizon. For RSS this is partial by construction: items that rotated out of the
  feed are gone, and `source_health.gap_reason` records that rather than implying recovery.

## License

MIT
