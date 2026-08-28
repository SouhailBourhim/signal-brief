# Phase 5 runbook — platform polish

Exit condition (SPEC §12): **14+ consecutive daily briefs**, and **each re-added component
has a written before/after justification**. The deliverable row names a dbt migration of
silver→gold, and Kafka + Structured Streaming *if and only if* §14's criteria are met.

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
- **dbt is measured and refused, not migrated. This is a correction to §12's Phase 5 row and
  it is the reason the row cannot be executed as written.** §14 gates dbt on the gold layer
  exceeding ~10 models. Gold holds **four tables** — and the sharper problem is that *there is
  no silver→gold SQL layer to migrate*. `gold.cluster_enrichment` is an LLM call,
  `gold.macro_observations` is a bitemporal Spark MERGE, `gold.brief_items` is a render-time
  record of what the reader was shown. dbt models are `SELECT` statements. Adopting it here
  would mean rewriting working Spark into SQL in order to justify the tool — precisely the
  move §2 and §14 exist to prevent. So the deliverable is the measurement and the written
  refusal (5.B), and §12's row is amended to say so rather than left to look unfinished.
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

*(pending)*

## 5.B — ADR-0013: what stayed out, and what the numbers were

*(pending)*

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

*(pending)*

## 5.D — Power BI, and the permission that was never present

*(pending)*

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
