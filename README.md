# Signal

*A daily tech / finance / economy brief, and the pipeline that earns it.*

Ingests public news, filings, and macro data on a 15-minute cadence; stores raw responses
immutably; collapses syndicated coverage into ranked story clusters; and publishes a brief
at 07:00 Africa/Casablanca with lineage, replay, cost controls, and locally run LLM
enrichment built in.

**Status: Phase 1, in progress.** Three pollers — Hacker News, SEC EDGAR, one tech RSS feed
— run as scheduled Lambdas and land raw payloads in S3; a local Spark job commits them to
`bronze.raw_documents` on Iceberg, registered in Glue. The infrastructure is Terraform, and
applied. No LLM yet. [`SPEC.md`](SPEC.md) is the specification;
[`docs/runbooks/phase-1.md`](docs/runbooks/phase-1.md) is where this phase stands, including
what the first live invocations broke.

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

SPEC §15: never publish a metric the pipeline cannot recompute. Current numbers, produced
by `make eval` and `make skeleton` on the Phase 0 fixture:

| Metric | Value | Source |
|---|---|---|
| Dedup precision / recall | 1.000 / 1.000 (n=55 pairs) | `make eval` |
| Dedup ratio | 11 articles → 7 clusters | `make skeleton` |
| Entity resolution | — | Phase 3 |
| LLM eval accuracy, cache-hit rate | — | Phase 4 |
| Cost per day, Athena bytes scanned, S3 egress | — | Phase 4 |

The dedup numbers are on a synthetic fixture and prove the harness runs, not that the
clustering is good. Phase 3 replaces them with ~200 real labeled pairs.

## Layout

| Path | Contents |
|---|---|
| [`SPEC.md`](SPEC.md) | The specification. Start here |
| [`src/signal_core/contracts.py`](src/signal_core/contracts.py) | The poll contract every source implements |
| `src/signal_core/` | Sources, transform, dedup, ranking, rendering, ops |
| `handlers/` | Lambda entry point — one artifact, N functions |
| `infra/terraform/` | `bootstrap/` (state backend), `main/` (everything else) |
| `evals/` | Labeled sets, scorers, and the accuracy floors CI enforces |
| [`docs/how-signal-works.md`](docs/how-signal-works.md) | What each phase is for, in plain English — no prior knowledge assumed |
| [`docs/decisions/`](docs/decisions/) | ADRs, including the ones that reversed earlier choices |
| `docs/archive/` | Superseded specs, kept for the decision trail |

## Adding a source

The design's central claim is that this takes 30 minutes:

1. Write `src/signal_core/sources/<name>.py` implementing
   `poll(config, state) -> (list[RawDocument], State)`.
2. Register it in `src/signal_core/sources/__init__.py` and `config.SOURCES` — declaring
   its **backfill horizon**, which determines what catch-up can honestly promise (SPEC §6.3).
3. Add one entry to the Terraform `sources` map.

## Replay and catch-up are different

- **Replay** — reprocess an interval from bytes already in `bronze/`. Always possible and
  deterministic for every stage except clustering and LLM enrichment, whose exact
  guarantees are stated in SPEC §12.
- **Catch-up** — re-fetch what was missed during downtime. Bounded by each source's
  backfill horizon. For RSS this is partial by construction: items that rotated out of the
  feed are gone, and `source_health.gap_reason` records that rather than implying recovery.

## License

MIT
