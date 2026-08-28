# Phase 4A runbook — publish

> **Amended 2026-08-24 — the brief sends at 16:00.** This runbook uses the current schedule.
> The send moved because the host sleeps through the small hours: on 2026-08-24 the scheduler logged
> nothing between 21:00 and 12:58 UTC, then resumed mid-stride and fired the whole chain
> at once, so the brief landed at 13:59. The containers never died — they were frozen
> with the host, which still reported them `Up`. See `airflow/dags/brief_dag.py`.

Exit condition (SPEC §12): the ranker over real clusters, the HTML brief with §11's health
footer **emailed at 16:00**, the maintenance DAG, and the carried-forward items — accepted by
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
  wiring over data that exists. This one needs prices, so 4A adds source #8. No client
  library: yfinance pulls pandas transitively and `tests/test_lambda_artifact.py` fails the
  build if the handler's import chain acquires it (ADR-0006's 250 MB ceiling), so the poller
  is a plain `httpx` GET against a JSON endpoint. *This decision was made twice* — see 4A.D,
  where the first choice turned out to be gated behind a browser challenge. ADR-0010 records
  both halves.
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

## 4A.B — The HN score-velocity poller *(done 2026-08-22)*

Carried since Phase 2, where it was found and recorded as structurally unavailable: the
existing HN poller walks item ids forward from a watermark and fetches each exactly once, so
**every story is captured at the moment it is created, when its score is 1.** One point per
story is not a slope. SPEC §12 carried it into 4A because the fix is a source change.

- [x] `sources/hn_scores.py` — reads `topstories.json` and re-fetches the ranked ids every
      poll. Re-reading an id already seen is the entire point, so this poller consults
      neither `State.seen` nor `State.watermark`. It is a separate source rather than a mode
      of `hackernews` because folding both in would leave `State` meaning two different
      things depending on which caller was reading it.
- [x] `parse/hn_scores.py` + `ParsedScoreSnapshot`. The parser test deliberately reuses
      `tests/fixtures/bronze/hackernews/story.json` — one endpoint, two readings, and
      reusing the fixture is what keeps that claim honest if the shapes ever diverge.
- [x] `silver.hn_score_snapshots`, third normalize pass, third `process` DAG task.
- [x] `TOP_N = 60`. The full list is ~500 ids; sampling all of them every 15 minutes is
      48,000 requests a day to watch the tail of a ranking that will never lead a brief.

### Three things this turned up that the plan did not anticipate

**1. The concurrency limit the sources map warned about.** Its own comment said six pollers
fit under a new account's limit of 10 *"and source #7 is where it stops fitting"*, because
all six collide at :00/:15/:30/:45. Source #7 is this one. Rather than requesting Service
Quota L-B99A9384, `hn_scores` is the first schedule in the map expressed as **cron** —
`cron(7,22,37,52 * * * ? *)` misses the pileup and misses every multiple of 5, which is where
`hackernews` lands. Peak concurrency is unchanged. The property, not the expression, is
asserted in `test_the_seventh_poller_does_not_collide_with_the_other_six`.

`test_freshness_sla_is_longer_than_the_poll_cadence` had to learn to read cron for this; its
cadence is now the longest gap between fires, which is the interval an SLA actually has to
survive.

**2. The articles pass would have reported a permanent parser failure.** `normalize_window`
reads every source partition, and an `hn_scores` row parses to zero articles — harmless,
except that it is still counted in `NormalizeResult.bronze_rows`. At ~240 documents an hour
that is a bronze count climbing against a flat article count, forever, which is precisely
the shape of a broken parser in a metric SPEC §11 expects someone to read. `NON_ARTICLE_SOURCES`
excludes it from the pass rather than parsing it to nothing.

**3. Rank-at-snapshot was dropped on purpose.** It is a real velocity signal and observable
only at fetch time — HN's item endpoint reports a score but not where the story currently
sits. Carrying it would mean a new column on `RawDocument`, whose docstring records that
fetch metadata is first-class fields *"rather than a loose dict"*, and whose table is the
immutable record every other stage replays from. SPEC §7.4 asks for "HN score slope", and
score is in the payload. Widening the bronze schema for a signal the spec did not ask for is
not a trade this phase needed to make.

The snapshots table also breaks its siblings' MERGE convention, and the reason is worth
stating: `silver.articles` and `silver.hn_comments` dedupe on the document's own id, because
a document exists once. **A snapshot is a measurement**, so the same story an hour later is a
new row, not a duplicate — deduping on `item_id` would keep one score per story and delete
the very slope the table exists to provide. The key is `ingest_id`, unique per fetch, which
keeps replay deterministic (SPEC §6.3).

## 4A.D — Market data, source #8 *(done 2026-08-22)*

SPEC §7.4's market-corroboration component asks "did the linked ticker move beyond its
normal range?", and the lake held no prices. This is the only ranker component that needed a
new source rather than wiring over data that already existed.

### What broke before a line was written

The plan said Stooq, on a packaging argument: `yfinance` pulls pandas transitively and
`tests/test_lambda_artifact.py` fails the build if the handler's import chain acquires it
(ADR-0006's 250 MB ceiling), while Stooq serves CSV that needs nothing past `httpx`. The
argument was sound. The premise was not — **Stooq now answers every request with a JavaScript
proof-of-work challenge**:

    $ curl -s "https://stooq.com/q/d/l/?s=aapl.us&i=d"
    <noscript>This site requires JavaScript to verify your browser.</noscript>

Tried with the project User-Agent, a browser's, and curl's default — identical. An HTTP GET
cannot clear it, and **it should not try**: the challenge is an access control the operator
deliberately erected, so solving it in a Lambda would be evading a stated "no automated
access" and would break the first time the challenge changed.

This is Phase 1's EDGAR lesson arriving a second time (`sources/edgar.py`: *measured against
the live endpoint, not inferred from the docs*). The difference is that this time the check
happened **before** the module was written rather than after it was deployed, which is the
only reason it cost twenty minutes instead of a debugging session against a live schedule.

The fix separates the library from the source. The pandas objection was always about
yfinance the *package*, never Yahoo the *data* — and Yahoo's chart endpoint serves daily
OHLCV as plain JSON with no key. The poller is an `httpx` GET; the parser is stdlib `json`.
ADR-0010 carries both halves of the decision, and the "what would reverse this" section now
names access as the first risk, because it has already been the failure once.

### Measured, and better than the design it replaced

A single `range=3mo` request returns **63 daily bars**. That was not the plan — the plan was
one bar a day accumulating into a history — and it matters: the corroboration threshold
compares the latest return against the trailing window's standard deviation, and both now
come out of the same response. **Market corroboration works on the poller's first day**
rather than after twenty days of accumulation. It also makes a missed day self-repairing,
since every fetch re-states the recent past (SPEC §6.3).

- [x] `sources/market.py`, `parse/market.py`, `spark/jobs/market.py`, `airflow/dags/market_dag.py`
- [x] `silver.market_observations`, MERGEd on `(ticker, trade_date)` with **UPDATE on match**
- [x] Fixtures captured live, both the success and the delisted-symbol case

### The one silver table that overwrites

Its siblings insert only, and `normalize_window`'s docstring says why: nothing in a published
article legitimately changes after first sight, so an UPDATE clause would be a way to
silently rewrite history. **Prices invert that.** A split restates every prior bar and the
restated number is the correct one — insert-only would leave the table holding pre-split
prices that no longer describe anything. So this table matches on `(ticker, trade_date)` and
updates, following `cost_snapshot.record`'s shape rather than `normalize_window`'s.

It is also deliberately *not* bitemporal, despite SPEC §9 filing `macro_observations` under
gold with `valid_time`/`known_time`. That is 4B's ALFRED work and it is a different claim:
ALFRED serves every vintage, so "what was knowable on date X" is answerable. Yahoo serves
only the current view of history, so a `known_time` column here would be a fiction meaning
"as of our last fetch" — which is not a vintage (SPEC §17).

### Two more things the schedule had to account for

**The concurrency budget again.** Source #8 lands at `cron(11 2 * * ? *)` — daily, after the
US close, before `resolve` (04:30) and `cluster` (05:00), and on a minute nothing else uses.
`test_the_phase_4a_pollers_do_not_collide_with_the_phase_1_2_six` now covers both 4A sources.

**A daily source breaks two monitoring assumptions built for 15-minute ones.** Health is
assessed over the closed prior *hour*, so `min_docs_per_window` is 0 like `rss_ars` — any
positive floor reports a daily source as thin in 23 hours out of 24, and the content-staleness
SLA is what actually catches a dead market feed. And `freshness_sla_seconds` sits at the 2x
floor the test enforces rather than the 3x the other sources use, because a multiple means
something different at this cadence: 3x a 15-minute poll is 45 minutes, 3x a daily poll is
three days of silence. 48 h is two consecutive missed runs — the first unambiguous signal,
where 30 h would also have fired on a run that merely started late.

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

## 4A.H — The ranker over real clusters *(done 2026-08-22)*

Five of SPEC §7.4's six components, and the two carried-forward defects that gate them.

| Component | Weight | Reads |
|---|---|---|
| `breadth` | 0.25 | `distinct_publisher_count`, now honest (see below) |
| `relevance` | 0.25 | the watchlist against the cluster's **highest-mention** entity |
| `recency` | 0.20 | `last_seen` |
| `velocity` | 0.10 | `silver.hn_score_snapshots` (4A.B) |
| `market_corroboration` | 0.10 | `silver.market_observations` (4A.D) |
| `feedback` | 0.10 | `gold.brief_items` (4A.I) |

**The distribution is a claim, so it is written down.** `relevance` and `breadth` lead
because "is this about something I care about" and "did independent outlets corroborate it"
are the two questions a brief exists to answer. `recency` is deliberately no longer second:
3.D found minutes-old EDGAR filings beating four-publisher stories four hours old, and said
the fix was competition from components that measure importance rather than freshness. That
is now assertable —
`test_relevance_can_outrank_a_fresh_single_publisher_filing` is exactly 3.D's scenario.

**Novelty stays out**, and `test_novelty_is_not_a_weighted_component` pins it. ADR-0009 put
every embedding in 4B behind Ollama; a lexical stand-in would score near chance (0.500
held-out recall vs 0.909, on a strictly easier question) while occupying a weight, and a
hand-set weight over a near-chance component is worse than an absent one — the score is only
explainable if every term in it means something.

### Two carried-forward defects, fixed inside the components they gate

**Salience vs. resolution** (3.E) is not a separate fix; it *is* how `relevance` and
`market_corroboration` read their entity. Both score the highest-mention entity rather than
any resolved mention, and `read_cluster_entities` already sorted by descending mentions — so
a photo credit at `mentions=1` cannot make an Amazon story about Getty Images. A watchlist
company mentioned in passing still scores 0.3 rather than 0: weak evidence is not absence of
evidence.

**Publisher-diversity inflation** (3.E) is `dedup.effective_publisher`. `transform.to_article`
sets `publisher_domain` from the *submitted* URL, so three Show HN posts about one project —
its site, its repo, a thread — counted as three independent outlets. They are one community's
attention. The mapping is keyed on `source_id`, not on a domain denylist, because the
property belongs to the source: anything whose documents are user submissions of other
people's URLs has this shape.

It is applied at ranking, not at parse: the submitted URL is a true fact about the document
and `silver.articles` keeps it (SPEC §6.2). What changed is what counts as *independent
corroboration*, which is a ranking question.

### What velocity cost that the plan did not budget for

`silver.articles` had no key back to the source's own id. `ParsedItem.external_id` has
existed since 2.B — its docstring says "kept for traceability" — and `to_article` dropped it,
so there was no way to join a cluster to the score snapshots taken of its Hacker News member.

`article_id` could not stand in: it is derived from content, so it changes when a headline is
edited, which is exactly when a story is developing and its velocity matters most.

So `external_id` is now a column on `silver.articles`, appended last because
`ALTER TABLE ADD COLUMN` appends and `MERGE ... INSERT *` is positional. **`ensure_tables`
now calls `ensure_columns` for the articles table**, which it did not before —
`CREATE TABLE IF NOT EXISTS` is a no-op against a live table, and 3.D found that the hard way
with a deployed table two columns behind its own DDL.

One near-miss worth recording: the column was first documented with a `--` SQL comment
*inside* `ARTICLES_DDL`. `spark/tables.py::ddl_columns` parses those strings line-by-line and
would have read `--` as a column name, breaking `ensure_columns` and `cluster.py`'s column
derivation in a place neither names the DDL. Caught by reading the parser rather than by a
test, which is the same lesson this phase keeps re-learning.

### `gold.brief_items`, written through Athena

SPEC §9's schema, and the row that makes the loop a loop: it records the ranking decision
with `score_components` intact, it is what `signal brief feedback` updates, and it is what
the next run's `feedback` component reads back.

Written with `run_query`, not Spark. `build.py` and `read.py` stay JVM-free by design —
their docstrings argue that starting Spark to render ten stories is a lot of machinery for a
SELECT — and Athena v3 writes Iceberg directly, so the 16:00 path gains no JVM boot.

**DELETE-then-INSERT rather than MERGE**, and the WHERE clause is the reason: `make brief`
twice in one morning must not double the rows *and* must not discard a mark already left. So
the delete removes only rows for that date with no feedback, and clusters already marked
today are not re-inserted. MERGE expresses the first half and not the second, because a
re-run legitimately changes `rank` and `score` for rows that should still be there.

Only clusters that were *shown* are recorded. `rank` positions every cluster and cuts at
`limit`; writing the tail would mean a table where almost every row describes a story nobody
saw, and the feedback component would then read marks against positions that never appeared.

### One generalization, one honest cost

3.D's `_read_entities` — degrade to nothing if the table does not exist yet, but only for
"no such table" — became `_optional_read`, because 4A added three more optional reads and
three copies of that try/except would eventually disagree about which errors are survivable.

The brief now runs **six queries instead of three**, and all six are charged to the footer.
A component that quietly reads a table the reader is never told about is a cost SPEC §10.3
would not see, so `test_run_writes_a_brief_...` asserts the summed figure.

## 4A.I — The feedback loop's recording half *(done 2026-08-22)*

`signal feedback <cluster_id> up|down`, plus `--list` and `clear`.

A CLI verb rather than a form: there is no web server anywhere in this architecture and
SPEC §4's diagram has no serving layer past Athena, so collecting two bits a day through one
would mean a component SPEC §14 would then have to justify.

**It reads before it writes, and reads back after.** Athena's `UPDATE` reports no
affected-row count, so a typo'd cluster id would otherwise exit 0 and leave the reader
believing a mark was recorded that never was —
`test_an_unknown_cluster_fails_loudly_rather_than_succeeding_silently` pins that it refuses
instead. The read-back is the same "verified, not assumed" habit this project already applies
to SNS subscriptions and, in 4A.J below, to the SES identity.

`--list` exists because cluster ids are content hashes. Nobody reads one off a page and types
it from memory, so the verb that needs one has to be able to show them.

## 4A.J — Email at 16:00 *(done 2026-08-22)*

- [x] `brief/mailer.py` — Gmail SMTP, with its app password read from SSM
- [x] `infra/terraform/main/mail.tf` — Gmail app-password parameter and verification check
- [x] `airflow/dags/brief_dag.py` — cron `0 16 * * *` Africa/Casablanca, build then mail
- [x] `mail_from`/`mail_to` on `Settings`, defaulting to `contact_email`

**Local, not a ninth Lambda.** ADR-0002's boundary decides it: the renderer runs locally and
holds the finished HTML in memory, so a Lambda mailer would exist only to re-read from S3
what the process that called it just produced, and would put the daily send behind a
deployment cycle. SPEC §13's layout says the same thing — `brief/ # ranker, renderer, mailer`.

**Cron, not asset-triggered**, and this is the phase's one genuinely arguable schedule.
`assets.py` anticipates the brief consuming `CLUSTERS_COMMITTED` and the dependency is real,
but SPEC §12's acceptance says "emailed at **16:00**" — a clock time, not "whenever
clustering last finished". Asset-triggering would also risk a second send if either upstream
table were rebuilt later the same morning by a manual trigger or a backfill, and a duplicate
brief in the inbox is worse than a late one. The assets are declared as inlets for graph
visibility, exactly as `cluster_dag` already does.

**Two tasks, because they fail differently.** A build failure is a data or query problem; a
send failure is almost always the SES identity not being verified. Only the second is worth
retrying, so only it has retries — and splitting them means the rendered file survives a send
failure, so `make brief-open` still works and the morning is not lost.

### The manual step, and why it cannot be automated

`aws_ses_email_identity` makes AWS email a confirmation link. Terraform cannot click it and
**cannot tell a pending identity from a verified one** — the identical blind spot
`monitoring.tf` documents for its SNS subscription, with the identical consequence: applying
cleanly proves nothing. `mail.tf` outputs the check:

    aws sesv2 get-email-identity --email-identity <address> --query VerifiedForSendingStatus

Until that returns `true`, `send_brief` fails with SES's own error, which names the address
and is more useful than anything the module could add.

**The account stays in the SES sandbox deliberately.** Leaving it means asking AWS for
production access, which grants the ability to mail strangers — a capability this system has
no use for. In the sandbox both ends of a send must be verified identities, which for a
self-addressed daily brief is exactly one, and that is also why `mail_from` and `mail_to`
both default to `contact_email`.

The IAM role is worth one note of honesty: the admin user already holds `AdministratorAccess`
and could send today with no Terraform at all. The role exists for the reason `query.tf`
gives for `signal-analyst` — *"'I query with an admin key' undoes least-privilege even when
the identity behind it is trustworthy"* — and the argument is stronger here, because unlike a
query, sending mail has an outward-facing side effect.

`moto`'s `mock_aws` covers `ses.send_email` with no dependency change. Checked against the
installed environment rather than assumed, since "it needs no new credential" was the whole
argument for SES over Gmail SMTP.

### The send worked for five days and the brief was never read *(2026-08-28)*

Everything above is accurate about SES and none of it was enough. The identity **was**
verified, the role **did** work, and `send_brief` never once failed. The reader's report was
"the email is still not being sent even when my laptop is up," and the first half of that was
wrong in a way worth recording: it was being sent, and quarantined.

What the account said when asked:

    aws ses get-send-statistics   -> 8 delivery attempts, 0 bounces, 0 rejects, 0 complaints
    aws sesv2 get-account         -> SendingEnabled true, HEALTHY, suppression list empty
    aws sesv2 get-email-identity  -> VerifiedForSendingStatus true
                                     DkimAttributes.SigningEnabled FALSE, Status NOT_STARTED
    dig TXT _dmarc.gmail.com      -> v=DMARC1; p=none; sp=quarantine

Five briefs, all in Gmail's Spam folder. The `From:` claimed `gmail.com` while the envelope
sender was `amazonses.com`, so SPF did not align; Easy DKIM was never enabled, so nothing was
signed for `gmail.com` either. DMARC failed both ways, and because `gmail.com` publishes
`p=none` Gmail quarantined rather than rejected — which is why there was never a bounce to
notice, and why every `mail` task was green.

Three things this cost, all of them the same mistake:

- **The claim two paragraphs up — "a send failure is almost always the SES identity not being
  verified" — is what made this hard to see.** It named the only failure mode anyone had
  thought of, so a verified identity read as "the mailer is fine."
- **The move to 16:00 was a real fix for a different bug and it masked this one.** The
  send was both late and undelivered; only the lateness got diagnosed, and moving the clock
  made the symptom "no email in the morning" become "no email in the afternoon."
- **`tests/test_mailer.py` passed the whole time,** correctly, because `moto` was mocking a
  layer at which an undeliverable message and a delivered one are the same object.

`ops/health.py` exists because a source can be stale-but-successful. The mailer had exactly
that failure mode and no equivalent — `FetchOutcome`'s discipline never crossed into the brief
package. ADR-0013 moves the send to Gmail's own submission service, where SPF and DKIM align
because the account sends as itself.

**The five-minute check that would have caught it, and now belongs in every send verification:**
open the delivered mail in Gmail → **Show original** → confirm `SPF: PASS`, `DKIM: PASS`,
`DMARC: PASS`. Nothing offline proves delivery; this does.

## 4A.E — The `project` cost tag, read back *(done 2026-08-22)*

SPEC §12 carried this forward and the carried item was right, but **not for the reason it was
written down.** The tag has been on every taggable resource since Phase 1 and was activated
as a cost-allocation tag on 2026-08-20 (1.B). The tagging was done. What was missing was
anything that reads it.

`ops/athena.py::athena_cost_usd` is a different number and it is worth being precise about
the difference: it is Athena's byte-scanned estimate for *one query*, computed locally from a
published rate. Exact for its purpose, and completely blind to Lambda invocations, S3 storage
and requests, DynamoDB, and data transfer. SPEC §10.3 asks what the *project* costs; only
Cost Explorer answers that, and nothing called it.

`ops/costs.py` does, filtered on the tag and grouped by service, with `signal cost`.

**Deliberately not in a DAG.** Cost Explorer lags up to 24 hours and restates as AWS
finalizes charges, so a daily task presenting it as a live metric would be publishing a
number stale by construction — SPEC §17's rule is that a metric the pipeline cannot recompute
does not get claimed. It is also billed per request ($0.01), which is nothing weekly and real
money hourly. Manual, like `make dictionary`.

Two details worth having written down, because both produce a wrong number silently:
`GetCostAndUsage`'s end date is **exclusive**, and MONTHLY granularity over a range crossing a
month boundary returns **one entry per month** — summing rather than taking the last is the
difference between a 30-day figure and half of one. An empty result also gets an explanation
rather than being rendered as `$0.00`, since cost-allocation tags take up to 24h to appear and
never apply retroactively; "this is free" is a claim §17 would not let stand unexamined.

## 4A.F — The maintenance DAG *(done 2026-08-22)*

Compaction, snapshot expiry and orphan cleanup over all twelve tables, nightly at 02:00, with
before/after file counts in `ops.maintenance_runs`. This is where SPEC §12's "compaction delta
measured" gets its number.

### The procedures were probed before the job was written, and two needed correcting

The plan flagged the `CALL` syntax as cited from general Iceberg knowledge rather than
verified against this repo's pin. Probing it first was the right call — the obvious form was
wrong twice:

| | probed against Spark 4.1 / Iceberg (ADR-0006) |
|---|---|
| `rewrite_data_files` | **silently no-ops** below `min-input-files` (default 5): returns `rewritten_data_files_count=0`, no error |
| `expire_snapshots` | worked as expected |
| `remove_orphan_files` | **refuses** any `older_than` under 24 h: `IllegalArgumentException: Cannot remove orphan files with an interval less than 24 hours` |

The orphan guard exists because a shorter interval can delete files an in-flight write is
about to commit, and it happens to match what this job wants anyway — so it is respected
rather than overridden. The rewrite no-op is the more dangerous of the two: it is easy to read
"0 files rewritten" as *nothing to do* when the real answer is *not enough fragments yet*, so
`min_input_files` is a named parameter and a test forces a real rewrite to assert a real
delta (6 files → 1).

Both corrections are pinned by tests rather than only described here.

### Three smaller decisions

**One task over all tables**, not one per table, matching `cluster_dag`'s preference for
fewer moving parts. `maintain_table` never raises — a sweep that abandons eleven tables
because the twelfth is locked has made the problem worse — so errors are recorded per table
and the DAG fails on the aggregate.

**A table that does not exist yet is skipped, not failed.** A fresh environment has no
`silver.market_observations` until the market DAG first runs, and a nightly job reporting
failures for tables nobody has built is one whose failures stop being read (SPEC §11).

**Cron at 02:00, not asset-triggered.** Iceberg's snapshot isolation makes compaction safe
alongside readers and writers, so the schedule is about resource contention, not correctness
— 02:00 is clear of the 02:11 market poll, the 03:30 market DAG, `resolve` at 04:30, `cluster`
at 05:00 and the 16:00 brief. `assets.py` already anticipated this hook.

`test_every_maintained_table_is_one_the_pipeline_actually_writes` asserts the hardcoded list
against the jobs' own table constants, because a table missing from a sweep degrades
quietly — slower queries and a growing bill, with no failure anywhere.

## Built, and what remains

SPEC §12's 4A row, item by item:

| Asked for | Where it is |
|---|---|
| Ranker over real clusters | `brief/ranker.py` — five of §7.4's six components (4A.H) |
| HTML brief with §11's health footer | `brief/render.py`, unchanged; the footer already printed fetch **and** content staleness (4A.A) |
| Emailed at 16:00 | `brief/mailer.py` + `airflow/dags/brief_dag.py`, Gmail SMTP from the local side (4A.J) |
| Maintenance DAG | `spark/jobs/maintain.py` + `airflow/dags/maintenance_dag.py` (4A.F) |
| Stale-but-successful feed detection | Already done in 1.E; the row was stale, not the code (4A.A) |
| HN score-velocity poller | `sources/hn_scores.py`, source #7 (4A.B) |
| `project` cost-allocation tag | `ops/costs.py` — the tag existed; nothing read it (4A.E) |
| Salience vs. resolution | Folded into `relevance` and `market_corroboration` (4A.H) |
| Publisher-diversity inflation | `dedup.effective_publisher` (4A.H) |
| EDGAR shaping | `dedup.accessions` + the positive rule ahead of the veto (4A.G) |

Everything green from a clean tree: `make test` (all suites incl. `-m spark`), `make lint`,
`make eval` (unchanged — dedup 0.962/0.568, entities 0.868/0.611), `make skeleton` and
`make skeleton-nospark` (both 11 articles -> 7 clusters, agreeing exactly, which is the claim
CLAUDE.md makes for them), `make lambda-package` (11 MB), `make tf-validate`.

**The acceptance test itself is not done, and cannot be rushed.** It asks for three mornings
read with marks recorded, plus a measured compaction delta. That is calendar time:

- [x] **Applied 2026-08-22.** 15 added, 7 changed, 0 destroyed — the seven changes being the
      six existing pollers picking up the new artifact (they ship from one zip) plus the
      scheduler role gaining invoke rights on the two new functions.
- [x] **SES identity verified**, `VerifiedForSendingStatus: true`, and a real brief sent
      end-to-end through `send_brief_file` (MessageId `010001a02a3ea271-…`).
- [x] **Both new pollers invoked live against their real endpoints** — the check that caught
      EDGAR's 403 in Phase 1 and Stooq's browser challenge in 4A.D:
      `hn_scores` returned 60 documents (`TOP_N` exactly), `market` returned 18 — which is
      the count of *uppercase* watchlist entries, `openai` and `anthropic` correctly not
      fetched. Zero errors on both.
- [x] **Compaction delta measured** — see below.
- [ ] Three consecutive days: confirm the 16:00 mail arrived, run
      `signal feedback <cluster_id> up|down` on at least one item, and log what the read
      surfaced in the daily-read table below — the format `phase-3.md` used, because that
      table is what SPEC §1's behavioural claim actually rests on.

### The compaction delta, measured 2026-08-22

SPEC §12's 4A acceptance asks for a number from a real run. First sweep against the deployed
lake:

| Table | Files before | after | delta |
|---|---|---|---|
| `bronze.raw_documents` | 140 | 43 | **97** |
| `ops.pipeline_costs` | 33 | 1 | 32 |
| `silver.dim_entities` | 32 | 1 | 31 |
| `ops.source_health` | 32 | 1 | 31 |
| `silver.articles` | 25 | 10 | 15 |
| `silver.parse_rejects` | 18 | 1 | 17 |
| `silver.hn_comments` | 20 | 12 | 8 |
| others (clusters, mentions) | 10 | 10 | 0 |
| **total** | **310** | **79** | **231** |

**231 files removed, 74% of them.** ~13.4 MB rewritten. The shape is what the writers
predict: `ops.pipeline_costs` and `ops.source_health` commit one tiny row per DAG run and
compacted 32:1, while the cluster tables — written with `overwritePartitions` — were already
compact and moved 0.

The second sweep moved nothing, which is the better proof: compaction is idempotent, so the
nightly job converges rather than churning.

### What broke on first real use

Two, and both only against the deployed warehouse.

**1. `remove_orphan_files` cannot run on S3, and every table reported it as a failure.**
The first sweep errored on all eleven tables with
`UnsupportedFileSystemException: No FileSystem for scheme "s3"` — *after* compaction had
already succeeded, so the delta above is real and the errors were about a later step.

The cause is that this is the one procedure that does not go through Iceberg's `S3FileIO`.
Finding unreferenced files means listing the table's directory, which goes through Hadoop's
`FileSystem` API — and `spark/session.py` ships `iceberg-aws-bundle` (Iceberg's AWS
integration), not `hadoop-aws`. Checked rather than assumed:
`Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")` throws. There is no Hadoop S3
filesystem on the classpath at all.

**Adding `hadoop-aws` was rejected.** It brings an AWS SDK that has to agree with the one in
`iceberg-aws-bundle` — precisely the jar-version failure class ADR-0006 exists to prevent —
and it would buy the reclamation of files left by interrupted writes, on a lake whose storage
is inside the free tier. Compaction and snapshot expiry, which are what actually govern query
cost and storage growth, both work.

So it is recorded as a **skip**, not an error, per table in `ops.maintenance_runs`. A nightly
DAG that goes red for a known and documented limitation is one nobody reads (SPEC §11). The
detection is on the behaviour rather than on the warehouse URI, so if `hadoop-aws` ever lands
for another reason this starts working with no change.

**2. 3.D's defect recurred, in the table written to record maintenance.** The first sweep
created `ops.maintenance_runs` with eleven columns. `skipped` was added an hour later while
fixing finding 1. Reading it back against the deployed table then failed —
**while every test passed**, because the tests always created the table fresh from the
current DDL.

This is exactly 3.D's *"a deployed table two columns behind its own DDL, discovered only when
the brief failed reading them"*, and the irony is that 4A.H had already added `ensure_columns`
to `normalize.ensure_tables` for this reason and I did not apply it to the new table.
`maintain.ensure_table` now calls it and returns what it added, and
`test_a_record_table_created_before_a_column_existed_gains_it` creates the *old* shape first
rather than the current one — which is the only way a test can see this class of bug at all.

### The daily read

| Date | Read | What it showed |
|---|---|---|
| *(pending — the clock starts 2026-08-23; see below)* | | |

**The clock did not start when this phase merged, and the reason is worth recording.**

4A merged on 2026-08-22 with every test green, CI green, and Terraform applied — and its
local half never ran once. Two independent faults, both found on 2026-08-23 (written up in
[`phase-4b.md`](phase-4b.md)):

1. **The `brief` DAG was paused.** Airflow pauses a newly-discovered DAG by default, and
   nothing here overrode that. So did `market` and `maintenance`. The 16:00 send — the one
   thing this acceptance actually turns on — had never fired.
2. **`gold.brief_items` did not exist.** Its `CREATE TABLE` was written in Trino's dialect
   rather than Athena's and fails on three separate points. Nothing caught it because the
   tests inject a fake Athena client that records SQL without parsing it, and the only code
   path that would have executed it was inside the paused DAG.

So the feedback loop this acceptance is built on had never worked, behind a green build and a
green AWS console. Both are fixed and verified against the real account: the brief builds
end to end (2,979 clusters, 1,031 company links, 8 sources 0 not ok, 10 items recorded) and
`signal feedback <id> up` records a mark and reads it back.

**The papercut below turned out to be the same root cause**, and it is no longer a papercut.

### The environment papercut was not a papercut

`make clean` was failing because `airflow/dags/__pycache__/` is owned by uid **50000** — the
Airflow container's user — so the host cannot delete the `.pyc` files inside it. This was
recorded here as cosmetic and left alone, on the reasoning that it "does not affect the
pipeline".

**It was one symptom of two faults that had stopped the pipeline.** The same sentence written
here — *the Compose containers write into the repo as a different uid* — is also why deleting
`.cache` breaks the bind mount, which is what silently killed `ingest_monitor` for ten hours
on 2026-08-22. And the fix guessed at above, "`DAGS_ARE_PAUSED_AT_CREATION`-adjacent Compose
config", turned out to be the literal fix for a *different* and larger problem: that setting
is exactly why this phase's DAGs had never run.

Both are fixed (`PYTHONDONTWRITEBYTECODE` in the Airflow environment, a guard in `make clean`,
and `DAGS_ARE_PAUSED_AT_CREATION=false`). The lesson is about the triage, not the fix:
**"does not affect the pipeline" was an inference, and checking it would have cost one
command.**

## Then

Phase 4A closes when the three mornings are read. Then **Phase 4B**, which ADR-0008 kept as
its own phase precisely so it could not be crowded out: the Ollama enrichment stage with its
content-hash cache, Pydantic validation and 100-example eval set (SPEC §7.3, gated by
ADR-0003's model pin — still `UNPINNED`), and the ALFRED bitemporal macro store (SPEC §8,
the README's lead differentiator). Its acceptance is the 30-day reproducibility backfill.

4B also inherits ADR-0009's two deferred items — the embedding branch behind `dedup.decide`,
and `?itemDescription` plus a wider candidate set for the resolver — and one this phase adds:

| Item | Recorded in | Gates |
|---|---|---|
| **Novelty is still absent from `WEIGHTS`** — it needs embedding distance over 30 days of cluster heads, and ADR-0009 put every embedding in 4B | 4A.H | §7.4's sixth component; the ranker is five-sixths of its spec |

Worth stating plainly rather than leaving implied: **4A ships a ranker that is missing a
component by design**, and the reason is on the record in two places rather than in anyone's
memory.
