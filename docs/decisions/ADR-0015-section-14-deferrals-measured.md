# ADR-0015 — §14's five deferrals, measured and refused

**Status:** Accepted · **Date:** 2026-08-29 · **Amends SPEC §14's pgvector estimate; corrects a stale paraphrase of §12 in two runbooks**

## Context

SPEC §14 opens with the sentence this record exists to honour:

> Deferral without a re-entry test is just a wish. Each of these returns when its criterion is
> met, and not before.

Phase 5's acceptance turns that into an obligation with two halves: each re-added component
gets a written before/after justification, **and each refused one gets the measurement that
refused it**. Five components were deferred. Five are refused below. None is refused on
taste, and the numbers are from the deployed lake on 2026-08-29, not from the estimates §14
was written with — one of which turns out to have been wrong.

| Deferred | Returns when | Measured 2026-08-29 | Verdict |
|---|---|---|---|
| **Kafka** | a genuinely continuous source **and** a second independent consumer of `articles.normalized` | 9 polled sources, 0 continuous; 1 deadline across the whole chain | **Refused, both clauses** |
| **Spark Structured Streaming** | Kafka returns | Kafka did not | **Refused** |
| **dbt** | gold exceeds ~10 models | **4** | **Refused** |
| **pgvector** | vector working set exceeds ~50k | **11,267** | **Refused** |
| **Ranker weight fitting** | feedback holds several hundred marks | **1 mark across 60 items** | **Refused** |

## The measurements

### Kafka — neither clause, and the second one moved further away this week

`config.SOURCES` holds ten entries, nine of them production (`fake` is the skeleton's). Every
one implements `poll(config, state)` and is reached by an EventBridge schedule. There is no
continuous upstream to preserve, which is what §14 predicted and what nine sources later still
describes.

The second clause is the interesting one. Nine modules read `silver.articles`:

| Module | What it is |
|---|---|
| `spark/jobs/normalize.py` | the **writer**, not a consumer |
| `spark/jobs/cluster.py` | the batch clustering job §14 explicitly excludes |
| `spark/jobs/resolve.py`, `spark/jobs/market.py` | further stages of the same nightly chain |
| `spark/jobs/maintain.py`, `spark/jobs/repair.py` | table maintenance, not stream consumption |
| `ops/reproduce.py` | a verification harness |
| `brief/read.py`, `brief/build.py` | the brief, at the end of the same chain |

Nine readers, and **zero independent consumers with a latency SLA of their own.** §14 asked
for "e.g. a live ticker-alert path with its own latency SLA"; what exists is one chain with
one deadline, the 16:00 brief.

**ADR-0014 made this less true, not more, and it did so five days ago.** That record replaced
five independent crons with a single asset-ordered chain — which is the precise opposite of
acquiring a second independent consumer. A topic introduced now would sit between two stages
of a chain that was just deliberately tightened into one.

### Structured Streaming — gated on Kafka, and Kafka did not return

§14 makes this conditional and nothing about the condition changed. Recorded rather than
skipped, because a deferral that quietly stops being tracked is the failure mode §12's
carry-forward rule exists to catch.

### dbt — 4 models, and no SQL layer to migrate

`SHOW TABLES IN gold` returns four: `brief_items`, `cluster_enrichment`, `enrichment_rejects`,
`macro_observations`. The gate is ~10.

**The sharper problem is not the count.** dbt models are `SELECT` statements, and there is no
silver→gold SQL to express as one:

- `gold.cluster_enrichment` is an Ollama call with a content-hash cache.
- `gold.macro_observations` is a bitemporal Spark MERGE.
- `gold.brief_items` is a render-time record of what the reader was actually shown.
- `gold.enrichment_rejects` is a quarantine table written on schema failure.

Adopting dbt here would mean rewriting working Spark into SQL **in order to justify the
tool** — the exact move §2 and §14 exist to prevent. The second half of §14's criterion
("hand-written tests start duplicating each other") is also unmet: the data-quality tests are
per-table and share no assertions.

**A note on what §12 actually asks for, because two runbooks say otherwise.**
`docs/runbooks/phase-4b.md` and `docs/runbooks/phase-5.md` both paraphrase §12's Phase 5 row as
naming "a dbt migration of silver→gold" as the deliverable, and phase-5.md's decisions section
goes on to call its own refusal "a correction to §12's Phase 5 row". Read as written, §12 asks
for something narrower and already correct:

> §14's re-entry criteria **measured and written up** — dbt, Kafka + Structured Streaming,
> pgvector, weight fitting — re-added only where a criterion is actually met

So nothing in SPEC needs amending on dbt's account: the measurement *is* the deliverable, and
this file is it. What needed correcting was the paraphrase, in both runbooks. Recorded because
the mistake is the interesting kind — a summary of a spec drifting into a stronger claim than
the spec makes, and then being planned against.

### pgvector — 11,267 vectors against a 50,000 gate, and §14's own estimate was low

| Working set | §14's estimate | Measured |
|---|---|---|
| clustering's 72-hour window | "a few thousand articles" | **1,888** |
| ~30 days of cluster heads | "~1k–3k vectors" | **9,379** |
| total | — | **11,267** |

The gate holds with 4.4× of headroom, so the decision is unchanged. **But §14's estimate for
the cluster-head set was out by roughly 3×**, and the number is corrected here rather than
left to be re-derived by whoever next reads that row expecting 3,000. At the current rate —
about 1,680 clusters per three-day window — the 50,000 gate is reached around **2026-12**, if
the corpus keeps growing at this rate and nothing is pruned. That is a date, which is what
§14 wanted the row to have.

Until then this is a numpy array and a cosine call, which is what 5.C implements.

### Ranker weight fitting — one mark

`gold.brief_items` holds 60 rows across six brief dates. `user_feedback` is non-null on
**one**. §14 asks for several hundred.

This is the one refusal that is not really about the criterion. Fitting weights to a single
mark is not a close call; the reason it is worth recording is *why* the table is nearly empty,
which is that the marks are made from a CLI (`signal feedback <cluster_id> up|down`) that
requires the reader to copy a cluster id that the brief did not, until this phase, print
anywhere on the page. The measurement refuses the component and simultaneously names the
defect that produced the measurement — see 5.C.

## Decision

**All five stay deferred, each with the number above recorded against its row.** SPEC §14's
table is amended in place with the measured values. §12 is left alone: it already asks for a
measurement rather than a migration. The two runbooks that said otherwise are corrected.

## Consequences

- Phase 5's acceptance clause "each refused one has the measurement that refused it" is
  satisfiable by pointing at this file.
- One SPEC correction is on the record rather than in a diff: §14's pgvector estimate. §12
  needed none — the drift was in the runbooks summarising it, and is fixed there.
- pgvector acquires an approximate return date (~2026-12) instead of an open condition, which
  is the difference between a deferral and a wish.
- Nothing here forecloses re-entry. Each row keeps its original criterion; only the evidence
  is new.
