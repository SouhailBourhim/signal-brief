# Signal

*A daily tech / finance / economy brief, and the pipeline that earns it.*

Ingests public news, filings, and macro data on a 15-minute cadence; stores raw responses
immutably; collapses syndicated coverage into ranked story clusters; and publishes a brief
at 07:00 Africa/Casablanca with lineage, replay, cost controls, and locally run LLM
enrichment built in.

**Status: Phase 2, done.** Six pollers — Hacker News, SEC EDGAR + Form D, and three RSS/Atom
feeds — run as scheduled Lambdas and land raw payloads in S3; a local Spark job commits them
to `bronze.raw_documents` on Iceberg, registered in Glue. A second Spark job, triggered
automatically off an Airflow Asset the moment a commit lands, parses that into
`silver.articles` and `silver.hn_comments`, and Athena answers ad-hoc questions against the
result with bytes-scanned and cost recorded for every query (`docs/athena.md`). The
infrastructure is Terraform, and applied. No LLM yet. [`SPEC.md`](SPEC.md) is the
specification; [`docs/runbooks/phase-2.md`](docs/runbooks/phase-2.md) is where this phase
stands, including what broke on first real use.

**Replay and catch-up are not the same promise.** Replay reprocesses an interval from bytes
already in `bronze/` — deterministic, and always available. Catch-up re-fetches what was
missed during downtime, and is bounded by each source's backfill horizon: Hacker News can be
recovered completely, EDGAR for about a day, and an RSS feed only as far back as its current
window. What catch-up cannot recover is recorded as a `gap_reason` per source per interval
in `ops.source_health` and printed in the brief's footer, rather than left to look like a
quiet day (SPEC §6.3).

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
| Dedup ratio | 11 → 7 clusters (fake) · 2,769 → 2,277 (real) | `make skeleton` / `signal brief` |
| Ingestion, one production window | 521 bronze rows → 207 articles (19 quarantined, all `hackernews`/dead-item) | `docs/runbooks/phase-2.md` 2.E, real AWS |
| Athena, `SELECT *` vs. projected vs. partition-pruned, same question | 184,259 / 73,373 / 64,713 bytes scanned | `docs/athena.md`, real AWS |
| S3 egress, one commit | 3,468,248 bytes | `ops.pipeline_costs`, real AWS |
| Entity resolution | — | Phase 3 |
| LLM eval accuracy, cache-hit rate | — | Phase 4B |
| Cost per day (full pipeline) | — | Phase 4A — pieces above are real, a full day's total isn't assembled yet |
| Consecutive daily briefs read | — | Phase 3 starts the count (SPEC §12's brief ladder) |

The fixture's 1.000/1.000 proved the harness runs, not that the clustering is good — and
Phase 3's 252 real labeled pairs showed how much daylight sat between those two claims. On a
base-rate sample the Phase 0 rule made 34 merges and **every one was wrong**, while missing
23 of 43 genuine same-story pairs; one cluster in the first real brief swallowed 64% of the
corpus. 3.B fixed both, and the numbers above are what replaced them. The bad ones stayed
published while they were bad — a metric you report only once it flatters you is not a
metric — and the whole arc is in [`docs/runbooks/phase-3.md`](docs/runbooks/phase-3.md).

Two caveats carried on purpose. **Quote the held-out row, not the full set**: half the full
set was fitted on, so 0.962 is optimistic by construction, while 1.000 / 0.500 was measured
on pairs the fitting never saw. And these labels were made by an LLM assistant and then reviewed by the reader, who overrode
three (`labeler` is stamped on every record, `reviewed_from` on every overridden one) — so
the figure measures agreement with a model on the bulk of the set, spot-checked where the
rule and the labeler disagreed.

Every Athena dollar figure behind the bytes-scanned numbers above floors at Athena's real 10 MB
per-query minimum (`ops/athena.py`) and currently rounds to the same $0.0000477 for all
three — the lake is still small enough that bytes scanned, not cost, is the metric that
actually moves; see `docs/athena.md` for why that's stated rather than hidden.

## Layout

| Path | Contents |
|---|---|
| [`SPEC.md`](SPEC.md) | The specification. Start here |
| [`src/signal_core/contracts.py`](src/signal_core/contracts.py) | The poll contract every source implements |
| `src/signal_core/` | Sources, transform, dedup, ranking, rendering, ops |
| `handlers/` | Lambda entry point — one artifact, N functions |
| `infra/terraform/` | `bootstrap/` (state backend), `main/` (everything else) |
| `evals/` | Labeled sets, scorers, and the accuracy floors CI enforces |
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
