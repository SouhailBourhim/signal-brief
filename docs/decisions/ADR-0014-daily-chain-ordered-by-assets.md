# ADR-0014 — The daily chain is ordered by assets, not by cron arithmetic

**Status:** Accepted · **Date:** 2026-08-29 · **Amends the scheduling half of ADR-0008**

## Context

Five stages built the brief's inputs, each on its own daily cron, each with a docstring
explaining why it was a cron and not triggered by an asset:

| DAG | Cron (UTC) | Its stated reason for not being asset-triggered |
|---|---|---|
| `market` | 02:30 | the poller fires once a day; hourly `BRONZE_COMMITTED` would boot a JVM 24x to find new bars once |
| `macro` | 02:40 | the series release monthly; re-merging unchanged vintages hourly is 24x the work for one brief |
| `resolve` | 03:30 | the product is read once a morning; recomputing on every hourly commit is work nobody sees |
| `cluster` | 04:00 | clustering reads a rolling 72-hour window; hourly recomputation is 24x the work |
| `enrich` | 05:15 | SPEC §7.3: enrichment runs "against cluster heads once per pre-brief window, not on every 15-minute cycle" |

**Every one of those reasons is correct, and none of them is an argument for a cron.** They
are arguments against the *hourly* assets — `BRONZE_COMMITTED` and `SILVER_COMMITTED`, which
`ingest_monitor` and `process` emit 24 times a day. What the crons were silently also doing was
encoding the *order* of the five stages, and the order was never written down anywhere except
as the arithmetic relationship between five separate cron expressions that have to be read
side by side.

That ordering held only while the machine was awake to observe it. ADR-0002 puts everything
interpretive on a laptop, and a laptop sleeps.

### What happened on 2026-08-29

The host was suspended from ~21:15 UTC on 08-28 until 13:24 UTC on 08-29, about sixteen hours.
The containers were frozen with it, not killed, so Docker reported them `Up` throughout and no
`restart:` policy was ever going to help. Ingestion was unaffected — the pollers run as Lambdas
on EventBridge in AWS, and they staged **449 objects** to S3 while the laptop was asleep.

On wake, every `catchup=False` DAG fired its most recent missed slot at once:

- `market`, `macro`, `resolve`, `cluster` and `enrich` all started at **13:24:14** and had all
  reported success by **13:26:38**.
- `ingest_monitor`'s `commit_staged` also started at 13:24:14 — and was still running. It spent
  eleven minutes syncing the backlog and then **failed** at 13:35:15 on S3 `PutObject` read
  timeouts (five attempts, one "Remote host terminated the handshake"), a network wobble on a
  just-resumed host. Iceberg aborted the write atomically, so bronze was untouched rather than
  half-written. The next hourly slot retried and committed successfully at **13:36:54**, in 96
  seconds, because the failed attempt had already warmed the staging cache.

So all five daily stages ran to green against **pre-sleep bronze**, ten minutes before the
night's documents existed locally. Nothing failed. Nothing alerted. The pipeline reported a
successful day and would have mailed a brief built on the previous day's data.

This is the second incident from this root cause. The first produced ADR-0008's amendment,
which moved the brief's send from 07:00 to 16:00 so a clock time would fall when the reader
is demonstrably at the keyboard. That was the right fix for the *send*, and it is untouched
here — but it treated one DAG's symptom, and left the five upstream stages ordered by nothing
more than the hope that the host would be awake for all five slots in sequence.

## Decision

**Order the daily chain with assets that only the daily stages emit, and gate its head on
bronze actually having been committed.**

Two new assets in `airflow/dags/assets.py`, deliberately separate from the hourly ones:

```
MARKET_DAILY = Asset("signal://daily/market-loaded")
MACRO_DAILY  = Asset("signal://daily/macro-loaded")
```

The chain becomes:

```
ingest_monitor ──BRONZE_COMMITTED──> process ──SILVER_COMMITTED──> (hourly, unchanged)
                       │
market (cron 03:30 local, gated on bronze freshness)
   └─MARKET_DAILY─> macro
        └─MACRO_DAILY─> resolve
             └─MENTIONS_RESOLVED─> cluster
                  └─CLUSTERS_COMMITTED─┐
                                        ├──(AND)──> enrich
                     MENTIONS_RESOLVED ─┘
brief: cron 16:00, unchanged
```

Three properties of this are the decision, and each is load-bearing:

1. **The daily stages hang off daily assets, never the hourly ones.** `MARKET_DAILY` and
   `MACRO_DAILY` are emitted once per day by one task each, so every stage still runs exactly
   once a day. The "24x the work" objection that each docstring raises is preserved in full —
   it was always an objection to the hourly assets specifically, and nothing here puts a daily
   stage on an hourly trigger. SPEC §7.3's "once per pre-brief window" is satisfied by the
   *rate*, which is unchanged; only the mechanism moved.

2. **`market` keeps a cron and gains a gate.** Something has to start the day, and a clock is
   the honest way to say "once, in the morning". But a cron only means what it says if the
   machine is awake, so `market`'s first task is a `wait_for_fresh_bronze` sensor that blocks
   until `bronze.raw_documents` has a snapshot newer than two hours. It reads Iceberg's
   `snapshots` metadata table, not the data: a poke costs a metadata read, and `max(fetched_at)`
   over the rows would answer a different question anyway — how new the *documents* are, which
   a source that stopped publishing also makes old. What the gate needs to know is whether the
   commit happened, which is a property of the table rather than its contents. It pokes every
   five minutes in `reschedule` mode and times out after four hours.

3. **`enrich` requires both `CLUSTERS_COMMITTED` and `MENTIONS_RESOLVED` (an AND).**
   `CLUSTERS_COMMITTED` alone would order it correctly in today's chain, but the brief joins
   enrichment to entities, and the AND is what stops a re-run of one silently pairing with
   yesterday's other.

**`brief` stays on its 16:00 cron and is not asset-triggered.** Its docstring already argues
this and the argument is unchanged: SPEC §12's acceptance is a clock time, and asset-triggering
risks a *second send* if an upstream is ever rebuilt later the same day. A duplicate brief in
the inbox is worse than a late one.

## Consequences

**The ordering is now a fact rather than an inference.** On a wake-up, Airflow can no longer
run `enrich` before `cluster`, because it has nothing to trigger it with until `cluster`
emits. The stampede that produced this incident is not mitigated, it is unrepresentable.

**The five stages no longer need to agree about the clock.** Their cron offsets — the 30
minutes between `resolve` and `cluster`, the 45 between `cluster` and `enrich` — were doing
real work, and that work is now done by the asset edges instead. Changing when the chain runs
is one edit to `market`'s cron rather than five that have to stay consistent.

**A failure now halts the chain, where before it degraded silently.** If `market` fails,
`cluster` does not run. This is the deliberate trade: the previous behaviour was five DAGs
independently succeeding against whatever data happened to be there, which is exactly how a
brief gets built on yesterday's bronze and mailed looking normal. A halted chain is visible;
a stale brief is not. The same reasoning governs the gate — timing out is a loud failure, and
a brief built on pre-sleep data is the failure it exists to prevent.

**The chain is only as ordered as its head is gated.** Ordering the five stages relative to
each other does nothing about the commit they all race; that is what `wait_for_fresh_bronze`
is for, and it is the piece to check first if this recurs. Two hours of tolerance assumes
`ingest_monitor` is hourly — if that schedule changes, `BRONZE_MAX_AGE_HOURS` has to change
with it.

**Still unmonitored.** Nothing alerts on a local DAG failing; the CloudWatch alarms watch the
Lambdas, which were healthy throughout this incident. A halted chain is visible in the Airflow
UI to someone who looks. That gap is real and this record does not close it.
