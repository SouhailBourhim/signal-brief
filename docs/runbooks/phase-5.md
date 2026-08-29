# Phase 5 runbook — platform polish

Exit condition (SPEC §12): **14+ consecutive daily briefs**, and **each re-added component
has a written before/after justification** — and each refused one the measurement that refused
it. The deliverable row asks for §14's re-entry criteria "measured and written up … re-added
only where a criterion is actually met".

*(Corrected 2026-08-29. This paragraph used to say the row "names a dbt migration of
silver→gold". It does not — see ADR-0015. The decision bullet below was written against that
misreading and is corrected with it.)*

The count started **2026-08-23** and stands at 3 (08-23, 08-24, 08-25). Fourteen unbroken
lands on **2026-09-05**. That is the whole shape of this phase: a long clock, a short
critical path, and one item — 5.A — whose job is to make the streak claim *true* rather than
merely uncontradicted.

Carried forward from 4A and 4B, each named again here because §12's rule is that a carried
item stays visible in the runbook it came from **and** in the one receiving it:

| Item | From | Where it lands |
|---|---|---|
| **30-day reproducibility backfill** | 4B acceptance | Stays open. Bronze starts 2026-08-18, so `signal reproduce --days 30` cannot run before **2026-09-17** |
| **100 enrichment examples + the `[enrichment]` floors** | 4B.G | 5.0 |
| **ADR-0009's embedding branch behind `dedup.decide`** | ADR-0009, 4B | 5.C |
| **§7.4's novelty component** | 4A.H, ADR-0009 | 5.C |
| **The resolver's `?itemDescription` and wider candidate set** | ADR-0009 | 5.C |
| **Nothing alerts on a local DAG failing** | 4B "what broke" | 5.A |

## Decisions taken before starting

- **Phase 4 is closed inside this phase, as 5.0, rather than beside it.** Both 4A and 4B
  shipped every module §12 asks for and neither acceptance is signed off. 4A's is three
  mornings read with marks recorded: the three briefs exist in `out/`, and the daily-read
  table is empty. 4B's needs 100 labeled enrichment examples, whose harness is complete and
  has never been run, and which were recorded as "blocked on Ollama" — **Ollama has been up
  since at least 08-25**. Two of the three gates stopped being blocked and nobody went back
  for them, which is the failure mode §12's carry-forward rule exists to catch. Doing this
  beside Phase 5 rather than inside it was considered and declined: an acceptance that
  belongs to no phase's row is one that gets found late.
- **dbt is measured and refused, not migrated** — which is what §12 asked for all along; the
  "correction to §12" this bullet used to claim was a correction to our own paraphrase of it
  (ADR-0015). §14 gates dbt on the gold layer
  exceeding ~10 models. Gold holds **four tables** — and the sharper problem is that *there is
  no silver→gold SQL layer to migrate*. `gold.cluster_enrichment` is an LLM call,
  `gold.macro_observations` is a bitemporal Spark MERGE, `gold.brief_items` is a render-time
  record of what the reader was shown. dbt models are `SELECT` statements. Adopting it here
  would mean rewriting working Spark into SQL in order to justify the tool — precisely the
  move §2 and §14 exist to prevent. So the deliverable is the measurement and the written
  refusal (5.B), which is ADR-0015.
- **Kafka stays out on both clauses, and the same is therefore true of Structured
  Streaming.** §14 requires a genuinely continuous source *and* a second independent consumer
  of `articles.normalized`. There are nine sources, all polled, and one consumer. Neither
  clause is close. Recorded with the numbers in 5.B rather than asserted.
- **ADR-0009's three items land here, together.** 4B deferred them with the argument that
  "the infrastructure is already there" is how a ten-item phase row gets built, and that was
  right for 4B — which carried two differentiators of its own. It is not right for a second
  time. All three ride on one embeddings call, all three move a published README number, and
  the ranker has now shipped two phases at five-sixths of its spec. Deferring them again
  would need a better reason than the one that has already been used once.
- **The local half gets both kinds of alerting, because it fails in two ways.** An Airflow
  `on_failure_callback` cannot fire when the scheduler is frozen with the host — which is
  exactly what happened on 2026-08-24, with the containers reporting `Up` throughout. So a
  callback for task failures *and* an alarm on the AWS side for the case where nothing local
  is running to notice. One without the other watches half the failure surface.
- **Power BI (ADR-0012) is folded in as 5.D rather than landed separately.** It reads; it is
  not a monitoring layer and nothing depends on it. It also fixes a live IAM defect and
  blocks the next `terraform apply`, which puts it *ahead of* 5.A in the sequence rather than
  beside it — see 5.D.

## Sequencing

Two hard orderings, both discovered rather than assumed:

1. **5.D's `terraform import` precedes 5.A's apply.** The existing `gold` database must be
   imported before the next apply or it fails with `AlreadyExistsException`. 5.A adds an
   alarm to `monitoring.tf`, so 5.A's apply is the one that would fail.
2. **5.C's diagnosis precedes 5.C's implementation.** Five of §7.4's six components read
   `0.00` on the real page. A sixth component does not fix a page whose other five are
   already dead.

Everything else runs against the clock rather than in a chain.

## 5.0 — Close Phase 4

*(pending)*

## 5.A — The streak, computed; the local half, loud

**Done 2026-08-29.** Both halves shipped, and the first one immediately contradicted the
paragraph at the top of this file.

### The streak, computed

This runbook opened saying the streak "stands at 3 (08-23, 08-24, 08-25)". It stands at 3, and
those are the wrong three days:

```
$ uv run signal streak
day 3  (6 briefs, longest 3)
  first 2026-08-23 · current run from 2026-08-27
  missed: 2026-08-26
  307 bytes scanned, $0.000048
```

**2026-08-26 has no brief and nothing noticed** — not the DAG, not the console, not this
runbook, which was written three days later and still counted it. That is the whole argument
for computing the number: a hand-maintained streak has exactly one failure mode, and it is that
nobody forgets to increment it and everybody forgets to reset it.

`ops/streak.py` holds the counting, pure and taking its `as_of`; `brief/read.py::read_brief_streak`
holds the one part that needs an AWS account. It reads `brief_date` from `gold.brief_items`,
which is the table's partition key, so the query has never scanned more than 307 bytes.

Three things the implementation had to get right, each of which would have produced a
plausible wrong number:

- **`current` and `is_live` are separate.** A run of 9 that ended a week ago is not a streak of
  9, and it is not a streak of 0 either. The page says `9 days (streak ended 2026-08-12)`.
- **Yesterday still counts.** The brief fires at 16:00, so for most of any given day the newest
  brief is legitimately yesterday's. A one-day tolerance, named and tested.
- **The brief being built right now is not yet in the table it is about to be in.**
  `build.run` writes `gold.brief_items` *after* rendering, deliberately, because the row records
  what the reader was actually shown. Without `read_brief_streak(including=...)` the page would
  have reported "day 2" on the third consecutive morning, every morning — wrong in the one
  direction §16.5 cares about.

The number is on the page twice: `day 3` in the header, and a footer line carrying the longest
run, the total, and **the missed days by name**. Named rather than counted, because "1 missed"
is the shape of a number nobody chases.

`tests/test_brief_read.py`'s cost assertion failed the moment the streak read was added, which
is exactly what that test was built to do — it asserts the *total* bytes across every charged
query so that an unaccounted read cannot slip into the footer's cost figure.

### The local half, loud

Two mechanisms, because the local half fails in two ways and neither covers the other:

| Failure | Watched by |
|---|---|
| a task fails, scheduler alive | `airflow/dags/alerting.py::on_task_failure` → `Signal/Local` `LocalFailure` → `signal-local-task-failed` |
| the scheduler is frozen with the host | absence of `LocalHeartbeat` for 3 h → `signal-local-not-running` |

The second is the one that actually happened. On 2026-08-24 the host was suspended and the
containers reported `Up` throughout, because they were frozen with it rather than stopped.
**No `on_failure_callback` can report that** — the process that would send it is the process
that is gone. So every DAG emits a heartbeat on success (`ops/heartbeat.py`), `ingest_monitor`
runs hourly and is therefore the metronome, and CloudWatch alarms on three missed beats with
`treat_missing_data = "breaching"` — the same argument `monitoring.tf`'s `poller_silent` alarm
already made for Lambda invocations, pointed at the half of the system that is not in AWS.

Each event publishes twice: bare, for the alarm, and dimensioned by DAG, for reading. An alarm
needs a concrete dimension set, and a metric published only as `DagHeartbeat{Dag=cluster}` would
go silent the day that DAG is renamed.

Applied 2026-08-29 — `2 to add, 9 to change, 0 to destroy`, the nine being pollers picking up a
`signal_core` that changed.

**Both alarms were then verified by accident, which is the best kind.** The two probe metrics
published while testing `publish_heartbeat` and `publish_failure` were real datapoints, so
within minutes:

```
signal-local-not-running    OK      1.0 datapoint, not less than threshold
signal-local-task-failed    ALARM   1.0 datapoint, >= threshold
```

The failure alarm fired, SNS delivered, and the whole path — local Python → CloudWatch →
alarm → email — was proven end to end without anything having to be broken on purpose. It
returns to `OK` after an hour with no further failures. **This closes the "nothing alerts on a
local DAG failing" item carried from 4B.**

## 5.B — ADR-0015: what stayed out, and what the numbers were

*(This section was drafted as "ADR-0013". That number went to the Gmail SMTP reversal on
2026-08-28 while this phase was still pending, so the record is **ADR-0015**.)*

**Done 2026-08-29.** All five of §14's deferrals measured against the deployed lake and all
five refused, each with its number:
[`ADR-0015`](../decisions/ADR-0015-section-14-deferrals-measured.md).

| Deferred | Gate | Measured | Headroom |
|---|---|---|---|
| Kafka | continuous source **and** a 2nd independent consumer | 9 polled sources, 0 continuous; 9 readers of `silver.articles`, 0 with their own latency SLA | neither clause |
| Structured Streaming | Kafka returns | it did not | — |
| dbt | gold > ~10 models | 4, and no silver→gold SQL to migrate | 2.5x |
| pgvector | working set > 50k vectors | 1,888 + 9,379 = **11,267** | 4.4x, gate ~2026-12 |
| weight fitting | several hundred marks | **1** across 60 items | not close |

Three things came out of doing this that the plan did not anticipate:

- **§14's own pgvector estimate was low by ~3x.** It guessed "~1k–3k vectors" for 30 days of
  cluster heads; the real number is 9,379. The refusal stands either way, but the row now
  carries a measured figure and an approximate return date instead of an open condition.
- **ADR-0014 moved Kafka's second clause further out of reach, five days ago.** Replacing five
  crons with one asset-ordered chain is the opposite of acquiring an independent consumer. The
  criterion is now further from being met than when it was written, which is worth more than
  another year of "still deferred".
- **The weight-fitting measurement names its own cause.** One mark exists because marking
  requires copying a `cluster_id` the brief never printed. That is a product defect, not a
  data shortage, and it is fixed in 5.C rather than filed as a gate that may never open.

## 5.C — The ADR-0009 trio, and the ranker the brief is actually running

The finding that opens this section, recorded before any work on it, because it came from
reading the brief rather than from a test — which is the argument §12's ladder makes:

**Every story on the 2026-08-25 page scored exactly `0.25`**, with
`breadth 0.00 · recency 0.00 · relevance 1.00 · velocity 0.00 · market_corroboration 0.00 ·
feedback 0.00`. §7.4 specifies six components. Five read zero and the sixth is saturated, so
the shipped ranker is **effectively single-component** — it is ordering the page by watchlist
membership and nothing else. Every test passes and both eval gates are green, which is the
same shape as 3.D and 4A's paused DAG: green build, green console, wrong page.

Two candidates to check first, neither confirmed: `cluster` runs at 05:00 and `brief` at
16:00; and `recency = max(0, 1 - age_hours / 24)` can only read 0.00 for stories ≥24 h old,
which would follow if relevance-dominated selection is filling the top ten with old
watchlist-matching stories. Diagnose it before adding novelty on top.

**The brief redesign made this finding harder to lose, not easier.** The page now prints only
the components that actually moved a story's score, so a single-component ranker renders as a
single component — `recency 0.98 · relevance 0.80` and nothing else — instead of hiding
inside a run-on line of six figures where five happened to read `0.00`. The full set is still
recorded unabridged in `gold.brief_items.score_components`, which is where the diagnosis above
should be run from. The dead components are still dead; they now look it.

### The diagnosis, run 2026-08-29

Six days of `gold.brief_items`, mean component value per brief date:

| Brief date | score | breadth | recency | relevance | velocity | market | feedback |
|---|---|---|---|---|---|---|---|
| 2026-08-23 | 0.280 | 0.067 | **0.000** | 0.98 | 0.000 | 0.183 | 0.0 |
| 2026-08-24 | 0.270 | 0.067 | **0.000** | 0.96 | 0.000 | 0.137 | 0.0 |
| 2026-08-25 | 0.261 | 0.000 | **0.000** | 0.94 | 0.264 | 0.000 | 0.0 |
| 2026-08-27 | 0.267 | 0.033 | **0.000** | 0.94 | 0.241 | 0.000 | 0.0 |
| 2026-08-28 | 0.275 | 0.067 | **0.023** | 0.88 | 0.341 | 0.000 | 0.0 |
| 2026-08-29 | 0.340 | 0.000 | **0.901** | 0.64 | 0.000 | 0.000 | 0.0 |

**Neither of the two candidates named above was the cause, and the fix had already shipped.**
`recency` reads exactly `0.000` only for stories ≥24 h old, and it did so for five consecutive
days, then jumped to `0.901` on 08-29 — the day [ADR-0014](../decisions/ADR-0014-daily-chain-ordered-by-assets.md)
replaced the five crons with an asset-ordered chain. Before that, `cluster` at 04:00 was
routinely reading the previous day's bronze, so every story the ranker saw at 16:00 was already
a day old and `recency` was structurally pinned at zero. The relevance-domination theory had it
backwards: relevance looked saturating (0.94–0.98) because it was the only live term, not
because it was crowding the others out. It fell to 0.64 the moment recency started competing.

**`breadth` is not a bug and cannot be fixed in the ranker.** The corpus cannot produce it:

| Source | Articles (72 h) | Distinct publishers |
|---|---|---|
| `edgar` | 1,132 | 1 |
| `hackernews` | 704 | 477 |
| `edgar_formd` | 363 | 1 |
| `rss_tech` | 58 | 1 |
| `rss_ars` | 40 | 1 |
| `rss_verge` | 34 | 1 |

64% of the corpus is SEC filings from one publisher, which are unsyndicated by nature. 30% is
HN submissions pointing at 477 distinct domains, which are unsyndicated in practice. **99.64%
of clusters hold exactly one publisher** (1,674 of 1,680); six hold two; none holds three. And
the clusterer is doing its job on the six it can — every one is a genuine ars/verge/techcrunch/HN
overlap ("Nvidia closes in on Hugging Face acquisition", "Apple One … prices increase by up to
20 percent"). §7.4 spends **0.25 of the score** — the joint-largest weight — on a signal this
source mix emits for 0.36% of clusters.

**`velocity` and `market_corroboration` work and never reach the page.** Today's build linked
1,090 companies across 828 clusters, found 45 clusters with an HN slope and priced 18 tickers —
and the top ten contained none of them. `market` fired on 08-23/24 (0.18, 0.14) and has read
zero since.

**`feedback` is one mark, and the reason is a product defect.** Marking needs a `cluster_id`
copied from somewhere; until the redesign the page printed none. Measured in ADR-0015.

### What this changes about the plan

The section was scoped as "add novelty on top". The diagnosis says otherwise:

1. **recency — already fixed**, by ADR-0014, five days before this section opened. No work.
2. **breadth — a weighting bug, not a ranking bug.** The fix is either more syndicated sources
   or a weight that reflects the corpus. Adding a seventh component while a quarter of the
   score sits on a dead sixth would make the arithmetic worse, not better.
3. **novelty, embeddings, the resolver** — ADR-0009's three carried items, unchanged, and now
   with a measured reason to expect them to matter more than breadth ever will here.
4. **feedback — surface the `cluster_id`** so the gate in §14 can actually open.

*(pending — implementation)*

## 5.D — Power BI, and the permission that was never present

**Done 2026-08-29 — and the sequencing constraint this section was scheduled around had
already cleared itself.**

The plan put 5.D ahead of 5.A because ADR-0012's last consequence says the existing `gold`
database "must be `terraform import`ed before the next apply, or the apply fails with
`AlreadyExistsException`", and 5.A's alarm would have been the apply that hit it. Checked
before doing anything else:

```
$ terraform state list | grep glue_catalog_database
aws_glue_catalog_database.bronze
aws_glue_catalog_database.gold      <-- already managed
aws_glue_catalog_database.ops
aws_glue_catalog_database.silver
```

`gold` was imported when ADR-0012 landed on 08-26, not left for this phase. `terraform plan`
confirms no drift: **0 to add, 9 to change, 0 to destroy**, the nine being pollers picking up a
new zip hash because `signal_core` changed. So the one hard ordering in this phase's Sequencing
section was real when it was written and stale by the time it was executed — worth recording,
because the cost of checking was one command and the cost of assuming would have been building
5.A's apply around a blocker that no longer existed.

### The permission, verified

ADR-0012's actual finding was sharper than the import: **`signal-analyst` could not read `gold`
at all** — the database had been conjured at runtime by `CREATE SCHEMA IF NOT EXISTS` as the
admin identity, and the analyst policy grants databases by enumeration. Same class as ADR-0005:
a permission that was never wrong, only never present. Verified end to end by assuming the role
rather than by reading the policy:

```
$ aws sts get-caller-identity --query Arn
arn:aws:sts::481879233905:assumed-role/signal-analyst/phase5-5d-verify

$ aws athena list-databases --catalog-name AwsDataCatalog --query 'DatabaseList[].Name'
bronze   gold   ops   silver

$ aws athena list-table-metadata --database-name gold --query 'TableMetadataList[].Name'
brief_items   cluster_enrichment   enrichment_rejects   macro_observations

$ # SELECT brief_date, count(*) FROM gold.brief_items GROUP BY 1 ORDER BY 1 DESC LIMIT 3
SUCCEEDED — 2026-08-29 10 | 2026-08-28 10 | 2026-08-27 10
```

Browse *and* read, through the `signal` workgroup, as the analyst. The catalog-discovery path
that no code had ever exercised (`signal athena-query` is handed a database name and never
browses) now returns all four databases.

**What is not done here, and deliberately:** the Power BI Desktop report itself. It runs on the
Windows host outside WSL2, the query set is committed in `analytics/powerbi/` and the connection
walkthrough is `docs/powerbi.md`. Nothing in the pipeline depends on it — that is ADR-0012's
whole point, and it is why this section closes on the AWS side being correct rather than on a
`.pbix` existing.

## 5.E — The README closes its own gaps

*(pending)*

## The daily read

SPEC §12's acceptance. Continues 4A's table; the count starts 2026-08-23.

| Date | Read | What it showed |
|---|---|---|
| *(pending)* | | |

## Then

Phase 5 closes when the fourteenth consecutive brief is read and §14's refusals are written
with their numbers. 4B's 30-day reproduce opens 2026-09-17 — **after** this phase's own gate
clears on 2026-09-05 — so it is carried in the table at the top rather than blocking here.

SPEC §19's definition of done is a stranger cloning the repo, understanding it in five
minutes, and seeing evidence the brief has been useful and reliable over multiple weeks.
After this phase, the only thing left between here and that sentence is calendar.
