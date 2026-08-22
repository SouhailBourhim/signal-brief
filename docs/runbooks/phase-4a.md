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

## 4A.C — The watchlist *(done 2026-08-22)*

- [x] `watchlist.toml` + `watchlist.py`. One file, three collections, two consumers —
      `relevance` and `market_corroboration` both read it, because two lists would
      eventually disagree about whether a company is interesting and the ranker would score
      it both ways in the same run.
- [x] `tickers()` needs no join. `entities/dictionary.py` fixed the namespace in 3.C —
      **UPPERCASE is a tradable ticker, `lower-kebab-case` is an entity without one** — and
      its docstring says it did so for exactly this component: *"a namespace where that
      question is answerable by looking at the id, rather than by a join that might come back
      empty."* A private company on the watchlist (`openai`) counts for relevance and is
      simply not fetched. That is a property of the id, not a branch.
- [x] `matched_technologies` returns which keywords hit rather than a bool, so the component
      can explain itself in `score_components` (§7.4's actual requirement).
- [x] `macro_series` recorded and inert until 4B.

## 4A.G — EDGAR shaping: one Form 4, indexed twice *(done 2026-08-22)*

3.E recorded "one Form 4 clusters twice, once per CIK" and gated the brief's top ten on it.

**The mechanism.** `_TOKEN`'s character class has no hyphen, so `0001872100-26-000003`
reaches `identifiers` as three separate digit runs, indistinguishable from the CIKs beside
them. EDGAR indexes one submission under every CIK it concerns, so the reporting person's
copy and the issuer's copy carry the same accession and different CIKs — and the identity
veto, which reads only `identifiers`, sees two different documents and refuses to merge them.

**The fix is a positive rule, not a weaker veto.** `Prepared.accessions` keeps the accession
whole, matched by regex against text rather than against tokens, and `decide` returns True on
set equality *before* the veto runs. ADR-0009 recorded that the veto "is now load-bearing for
a stage that does not exist yet" — 4B's embedding branch measured a 14x worse corpus
false-merge rate without it, all of it EDGAR — so loosening it was not available.

Equality rather than intersection, for a reason the existing regression test makes concrete:
the two Allspring filings pinned by
`test_two_filings_by_one_company_are_two_stories` **already share two of three fragments**,
because a filer's own CIK is also its accession prefix. An intersection rule over `identifiers`
would have merged 47 distinct filings into one story. The test that has guarded this since 3.B
would have caught it; it is worth saying that the naive version of this fix was tried against
that test first and failed it.

### What the fixture already knew

The defect did not need a synthetic reproduction. The EDGAR feed committed in 2.B has been
carrying it since:

| | |
|---|---|
| Entries in `tests/fixtures/bronze/edgar/feed.xml` | 40 |
| Distinct filings those entries represent | **19** |
| Entries that are *not* part of a duplicate group | **0** |
| Pairs the accession rule newly merges | 24 |

Every entry in the feed is a duplicate of another. Eighteen accessions appear twice; one
appears four times — a Form 4/A with three co-reporting persons plus the issuer, which is
also the case that shows the rule has to work n-way rather than pairwise:

    0001104659-26-098473   4/A - Snyderman David J. (0001953511) (Reporting)
                           4/A - CoreWeave, Inc. (0001769628) (Issuer)
                           4/A - Supernova Management LLC (0001368026) (Reporting)
                           4/A - Magnetar Capital Partners LP (0001353085) (Reporting)

EDGAR was contributing roughly **2.1x its real filing volume** to clustering, and `breadth`
counts members. 3.D's "nine of the ten stories were SEC form numbers" had a second cause
underneath the one it fixed.

### `make eval` is unchanged, and that is the finding

    dedup  n=252  precision=0.962  recall=0.568  (tp=25 fp=1 fn=19)   — before and after

Byte-identical across the change. The plan predicted this and treated it as the reason to
run it, but the honest reading is sharper: **the labeled set contains none of these pairs**,
so it cannot certify this fix in either direction. That is 3.B's finding recurring
(*"the pairwise eval cannot certify the clustering"*) and it is why the evidence here is a
corpus count over real captured bytes plus a fixture-derived regression test, not a green gate.
The gate's job was to show the change did not cost anything elsewhere. It didn't.

## Then

*(open — closed when the three-morning acceptance completes)*
