# ADR-0008 — Split Phase 4, and land a real brief before it

**Status:** Accepted · **Date:** 2026-08-20

**Amended 2026-08-24:** the send this record refers to now runs at **16:00**. Only the clock time changed — the reasoning below stands as written. The host slept through the former early-morning schedule, so the scheduler was frozen (not stopped) and the brief arrived mid-afternoon anyway; `airflow/dags/brief_dag.py` carries the incident.

## Context

SPEC §12's phase table was written before Phases 0-2 were built. Three phases in, it has
drifted from what is actually true, and its Phase 4 row has accumulated more than it can
carry.

**Phase 4 held ten deliverables** — Ollama enrichment, its cache, its eval set, the ranker,
the HTML brief, scheduled email delivery, the ALFRED bitemporal store, the
maintenance DAG, CI, and cost/egress tracking — against roughly four in each of Phases 1,
2, and 3. Two of the ten are **differentiators #3 and #4 from §2**, the ones the README is
supposed to lead with. They were sharing a row with routine plumbing.

Four known-open items also land in Phase 4 without appearing in its row:

| Item | Recorded in | Gates |
|---|---|---|
| 1.D, the day-long switch-off test | `docs/runbooks/phase-1.md` | The README's replay/catch-up claim (§16 item 6) |
| Stale-but-successful feed detection | `docs/runbooks/phase-2.md` | The brief's health footer making freshness claims (§11) |
| HN score-velocity poller | `docs/runbooks/phase-2.md` | §7.4's velocity ranking component |
| `project` cost-allocation tag | `docs/runbooks/phase-1.md` | §10.3's per-project cost answer |

The velocity one is the sharpest: §7.4 lists Velocity as a ranking component whose primary
signal is HN score slope, and `sources/hackernews.py` walks item ids **forward** from a
watermark — every item is fetched exactly once, at creation, when its score is 1. There are
no second snapshots to slope against. Phase 4's ranker had a hard dependency on a
source-design change that was not in Phase 4's deliverable list.

Separately, and more structurally: **§1 defines success behaviourally** — *"the project
works when you read it daily for a month without maintaining it"* — and the first real brief
did not arrive until the end of that ten-item phase. The one metric that cannot be
compressed by working harder had the shortest runway of anything in the project. §7.4's
hand-set ranking weights would have been set against zero mornings of real reading, and
§14's re-entry criterion for automated weight fitting ("several hundred marked items") was
unreachable in practice.

Phase 0's walking skeleton does render a brief, but from the fake source. There was no
point in the plan where a **real but ugly** brief existed.

## Decision

Four changes to SPEC §12. No phase is reordered; the layer ordering (ingest → lake →
cluster → enrich/publish) is unchanged and is not in question.

### 1. Phase 4 splits into 4A and 4B

- **4A — Publish.** Ranker, brief from real clusters, scheduled email delivery, maintenance DAG, and
  the four carried-forward items above. Gate: three mornings read with feedback recorded,
  1.D proven against the deployed pipeline, compaction delta measured.
- **4B — Enrich + macro.** The Ollama stage with cache, schema validation and evals; the
  ALFRED bitemporal store. Gate: the 30-day backfill reproducibility test, **wording
  unchanged** — its precision about which stages are bit-reproducible and which are not is
  the point (§18).

Numbered 4A/4B rather than 4 and 5 deliberately: renumbering would invalidate roughly
thirty "Phase 4" / "Phase 5" references across ADR-0001, ADR-0003, `evals/`, and code
comments, for no gain. Existing references to "Phase 4" continue to mean the enrich-and-
publish work as a whole. Where the distinction matters, **ADR-0003's model-pin gate binds
4B**, not 4A.

### 2. A real brief lands at the *start* of Phase 3, not the end of Phase 4

`3.0` is now the first item in Phase 3: point the existing renderer at real
`silver.articles` instead of the skeleton's local Parquet. No clustering beyond Phase 0's
lexical `group_stories`, no enrichment, no email — `make brief` against real data, read in
a browser.

This is cheap in a way that matters to the argument: `brief/ranker.py` (breadth + recency,
`score_components` already a map), `brief/render.py`, and `templates/brief.html.j2` all
exist and have been running since Phase 0. 3.0 is a wiring job, not new code.

The brief then improves monotonically rather than appearing at the end: **3.0** real
articles, ugly ranking → **3.x** real clusters and resolved entities → **4A** the real
ranker, health footer, and email → **4B** enrichment and macro revisions. Every later phase
improves something already being read every morning.

### 3. §10.4's credit experiment leaves Phase 5 for a dated deadline

§10 states the clock — credits last *"6 months or until credits deplete, whichever comes
first"* — and §18 lists it as a risk, but §12 placed the experiment in the **last** phase,
behind the largest one. If Phase 4 ran long, the experiment would not have been delayed; it
would have become **impossible**, and "I evaluated X, here is what it cost and why I chose
Y" would be gone permanently.

It has no dependency on any other phase. It is now a standalone item with a date rather
than a phase position. First `terraform apply` was 2026-08-18, so the ceiling is
**approximately 2027-02-18** — the exact credit expiry must be read off the AWS billing
console and written into §10.4, not inferred from the first apply.

### 4. Labeling becomes scheduled work, starting now

`evals/dedup/pairs.jsonl` holds **55** synthetic Phase 0 pairs against §7.1's target of
~200. `evals/entities/` and `evals/enrichment/` are empty, and `evals/thresholds.toml` has
both floors pinned at `0.0` waiting for them. That is ~600 hand labels by one person,
gating two phase acceptances, currently implied as a sub-task of "build the clusterer"
rather than tracked as work with a duration.

Two changes. Labeling **starts against the real `silver.articles` data that already
exists** (2,397 rows as of 2026-08-20) rather than beginning when the clusterer is done —
20 pairs a day through Phase 3's build reaches 200 by the time the code needs them. And
labeling happens **before** the matching algorithm is written, not after, so the labels are
not quietly shaped to flatter the implementation they will judge.

## Consequences

- SPEC §12 gains a Phase 0 row and reconciles three deliverables that shipped in phases
  other than the one they were listed under: **CI** (shipped Phase 0, listed Phase 4),
  **cost and egress tracking** (shipped 2.D, listed Phase 4), and the **eval harness**
  (shipped Phase 0, listed Phase 3/4). §12 is the section that says what to do next; a
  stale one is worse than a terse one.
- §8's "it can be Phase 4 without being at risk" is revised. The claim was correct about
  *data* risk — ALFRED serves every vintage, so the store is backfillable from a standing
  start — and wrong about *schedule* risk: sharing a row with the entire publishing path is
  what put it at risk, not what protected it. It is independent of clustering, entities,
  and enrichment, and could be built any time after Phase 1.
- The §7.4 feedback table accumulates real marks from Phase 3 onward instead of from the
  end of Phase 4, which is the only way §14's "several hundred marked items" re-entry
  criterion becomes reachable rather than theoretical.
- 4A's gate now fails if 1.D is still open, so the README cannot claim a replay/catch-up
  guarantee the project never demonstrated end to end. That is §18's last bullet enforced
  by the phase table instead of by memory.
- The risk this accepts: a deliberately bad brief read every morning for two phases could
  sour the habit §1 depends on. Judged the smaller risk. Discovering the brief is not worth
  reading is information worth having in Phase 3, when sources and ranking are still cheap
  to change, rather than after all four differentiators are built.
