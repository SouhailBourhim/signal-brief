# Phase 4A runbook — publish

Exit condition (SPEC §12): the ranker over real clusters, the HTML brief with §11's health
footer **emailed at 07:00**, the maintenance DAG, and the carried-forward items — accepted by
**three mornings read running with the feedback loop recording marks**, and a measured
compaction delta.

Carried forward from Phase 3: nothing blocking. The four items SPEC §12's table still lists
(HN velocity poller, `project` cost tag, salience, publisher-diversity) plus 3.E's EDGAR
shaping are this phase's work, not prerequisites for starting it.

## Decisions taken before starting

- **Novelty waits for 4B.** SPEC §7.4 lists it first, and it is the one component that cannot
  be built here: it needs embedding distance over 30 days of clusters, and ADR-0009 already
  placed *every* embedding in 4B behind Ollama rather than dragging a second inference stack
  in early. Building a throwaway encoder to serve one ranking component would spend the
  1.1 GB ADR-0009 declined to spend, one phase before the stage that pays for it. `WEIGHTS`
  ships without it and says so.
- **Market corroboration ships, and brings a source with it.** The other four components are
  wiring over data that exists. This one needs prices, so 4A adds source #8. Stooq over
  yfinance: yfinance pulls pandas transitively, and `tests/test_lambda_artifact.py` fails the
  build if the handler's import chain acquires it (ADR-0006's 250 MB ceiling). Stooq is CSV
  over HTTP and needs nothing past `httpx`. ADR-0010 records it.
- **Email sends from the local side.** ADR-0002 puts ingestion in AWS because it must run
  whether or not the laptop is on, and everything interpretive locally. The brief is rendered
  locally, so it is mailed locally — SES via boto3 on the credentials Athena already uses, not
  a ninth Lambda whose job is to re-fetch what the renderer just had in memory.
- **The feedback loop is a CLI verb.** There is no web server in this architecture and SPEC §4's
  diagram has no serving layer beyond Athena. A `signal brief feedback` verb beside
  `signal athena-query` records marks without inventing a component SPEC §14 would then have
  to justify.

## 4A.A — Housekeeping *(done 2026-08-22)*

- [x] SPEC §12's carried-forward table reconciled against the code. Two edits, opposite
      directions:
      - **Stale-but-successful feed detection came off it.** The row says "measure staleness
        from `last_content_change_at`, not `last_success_at`" and that is what
        `ops/health.py::assess_source` already does — fixed in 1.E on 2026-08-20, with
        `dead_feed` in `DEGRADED_STATUSES` and `brief.html.j2` printing fetch staleness and
        content staleness as separate columns. `phase-2.md`'s own entry is headed *"Resolved"*
        and points at 1.E. The row outlived its defect by two days and one phase boundary.
      - **EDGAR shaping went on it.** `phase-3.md`'s closing "Then" names three items 3.E
        added; SPEC §12 carried five rows and had only two of them.
- [x] This runbook opened.
- [x] ADR-0010 — Stooq over yfinance, SES from the local side, novelty's deferral.

### What checking cost, and what it bought

The plan for this phase budgeted an implementation task for stale-feed detection. It was
already built. The only thing that found that was reading `assess_source` and the footer
template against the row that claimed otherwise — SPEC §12's table, ADR-0008, and
`docs/how-signal-works.md`'s status table all still described it as open, because three
documents were written the day before the fix and none were revisited after it.

That is the same shape as 3.D's finding (a deployed table two columns behind its own DDL) and
it points the same direction: **the docs are a claim about the code, and a stale claim reads
exactly like a live one.** Worth stating here because 4A adds four more documents that can
drift.

## Then

*(open — closed when the three-morning acceptance completes)*
