# Phase 2 runbook — lake + query

**Exit condition met, 2026-08-19.** Six sources, Spark normalize into `silver.articles` on
Glue-registered Iceberg, Athena serving queries, partitioning rationale documented (SPEC §12)
— and the acceptance test: **a stranger runs `make up`, answers an ad-hoc question in Athena,
and the bytes scanned and cost of that query are recorded.** `docs/athena.md` is the record;
2.F below is where it was closed out.

**Carried forward from Phase 1:** 1.D, the day-long switch-off acceptance test, is deferred by
decision. It gates the README's replay/catch-up claim (SPEC §16 item 6), not any Phase 2 work.
It stays open in [`phase-1.md`](phase-1.md).

## Decisions taken before starting

- **Six sources, not SPEC §3's five.** Form D and The Verge are what §3 asks for; Ars Technica
  is deliberately source #6, so §3's *"adding source #6 must be a 30-minute job"* becomes a
  measurement instead of an assertion. SPEC §3 amended to match.
- **`silver.articles` partitions on `days(event_date)`**, where
  `event_date = coalesce(published_at, fetched_at)` — not §9's `days(published_at)`.
  `published_at` is nullable by design (§6.2 trusts no timestamp), and a null cannot be pruned,
  so every source that omits or mangles a date would land in one partition that grows forever
  and is scanned by every date-bounded query. Deviation recorded in **ADR-0007** with the
  bytes-scanned measurement that justifies it.
- **Hacker News comments get their own `silver.hn_comments` table.** Not articles — a dense id
  walk is ~90% comments, and mapping them into `articles` would inflate "articles in" roughly
  10x and make §15's dedup ratio meaningless. Not dropped either: see the velocity finding
  below, which is the reason the table earns its place. Added to SPEC §9.
- **Athena query results live in their own bucket**, not a prefix under bronze. Bronze is
  `prevent_destroy` and framed as the immutable record; query scratch is neither.

## 2.0 — One repository, not two *(done 2026-08-19)*

Work started against a second clone at `C:\Users\bourh\Desktop\Signal`, discovered to be a
separate checkout at the same commit as `~/signal`. It had no `uv`, no Terraform and no Docker,
so it could not run `make test`, `make lint`, or a `terraform plan` — and it held the only copy
of `CLAUDE.md`, uncommitted.

- [x] Ported the changed files and `CLAUDE.md` into `~/signal`, which is canonical per the
      Phase 0 requirement CLAUDE.md itself states. The Desktop copy is stale and should be
      deleted rather than pulled.

This is worth a runbook line because it is the exact failure the "clone inside WSL2" rule
exists to prevent, and it still happened — the rule stops `/mnt/c` *paths*, not a second clone.

## 2.A — Sources four, five, and six *(done 2026-08-19, applied)*

- [x] `edgar_formd`, `rss_verge`, `rss_ars` — each a ~6-line binding of `sources/feed.py`,
      exactly like `rss_tech.py`. No new fetch logic was written, which is the contract in
      SPEC §6.1 doing its job
- [x] One `REGISTRY` entry, one `SOURCES` entry, one `var.sources` entry each — nothing else
- [x] `terraform apply`: **18 added, 4 changed, 0 destroyed**. The four in-place changes are the
      three existing Lambdas taking a new `source_code_hash` (one artifact serves all six
      functions) and the scheduler policy gaining three ARNs
- [x] 18 alarms (3 per source × 6), all six schedules `ENABLED`, SNS subscription re-verified
      `PendingConfirmation: false` — Phase 1's trap, re-checked because the apply touches the
      topic's neighbours
- [x] **First live invocation of all three**, because Phase 1's lesson is that this is where
      problems appear, not in tests. All three returned 200

### §3's 30-minute claim, measured

**08:44:08Z → 09:00:18Z — 16 minutes 10 seconds for all three sources**, including writing the
modules, the config and Terraform entries, a parity test, `terraform apply`, and live
verification against real endpoints. The claim was one source in thirty minutes; the measured
figure is three in sixteen. Recorded here so the README can state it rather than assert it.

### What the endpoints actually returned

Measured 2026-08-19, not taken from documentation — the Phase 1 EDGAR lesson.

| source | format | validators served | conditional GET |
|---|---|---|---|
| `edgar_formd` | Atom, **`encoding="ISO-8859-1"`** | neither `ETag` nor `Last-Modified` | **inert** — every poll is a full body |
| `rss_verge` | Atom, UTF-8 | weak `ETag` (`W/"..."`) | `If-None-Match` |
| `rss_ars` | **RSS 2.0**, UTF-8 | `Last-Modified` only | `If-Modified-Since` |

Three things follow that matter downstream:

- **`rss_ars` is the first source to exercise the `If-Modified-Since` branch.** Phase 1 only
  ever proved `If-None-Match`. Its first scheduled poll stored the validator and the manual
  invocation 40 seconds later returned **0 documents with `consecutive_failures: 0`** — a 304,
  confirmed independently with a direct conditional `curl`. That zero is the conditional-GET
  path working, which is exactly the distinction `FetchOutcome.NOT_MODIFIED` exists to preserve.
- **Both SEC sources are unconditional by construction.** `browse-edgar` is a CGI script and
  serves no validator, so every poll transfers a full body and content movement is detectable
  only through `content_hash` — never through a status code.
- **Form D's declared encoding is ISO-8859-1.** This is the concrete case
  `staging.to_record`'s base64 exists for (SPEC §6.1): the bytes are the record, and decoding
  is interpretation that belongs in Spark against stored bytes, where a mistake is re-runnable.

### The apply proved the alert path end to end, by accident

Each new source emitted a `signal-poll-<id>-not-running` **ALARM → OK** email within minutes of
the apply. This is expected, not a fault: the alarm is created with
`treat_missing_data = "breaching"` (silence *is* the condition being watched), so a brand-new
alarm with no `Invocations` datapoint yet starts in ALARM and flips to OK as soon as the first
poll lands. Three new sources, three round trips, all self-resolving.

Worth recording because it closes a gap Phase 1 left open. Phase 1 verified the SNS subscription
with `get-subscription-attributes` reporting `PendingConfirmation: false`, which proves the
subscription is *confirmed* — it does not prove a message ever reaches the inbox. An email
arriving does. The alerting path is now verified end to end rather than at the API.

The papercut is that adding a source generates one spurious ALARM email. The alternative,
`treat_missing_data = "notBreaching"`, would defeat the alarm's entire purpose, so this stays.

### Backfill horizon: Form D is `DAY`, not `COMPLETE`

SPEC §3 lists Form D as complete. That is true of the daily full-index files and **not** of the
current-filings feed this polls. Declaring `COMPLETE` would make `plan_catch_up` promise a
recovery it cannot perform and suppress the `gap_reason` §6.3 exists to surface, so the config
declares what is true of the endpoint we actually hit. The index fallback is Phase 4+.

### Concurrency

Six unreserved functions against a new account's total limit of 10, all colliding at
`:00/:15/:30/:45`. It still fits. **Source #7 is where it stops fitting**, and where Service
Quota `L-B99A9384` stops being optional — see `phase-1.md` for why the reservation is `-1`.

## 2.B — Parsers: bronze bytes → item records *(done 2026-08-19)*

New `src/signal_core/parse/` package with a `REGISTRY`/`get_parser` mirroring `sources/`.
Without it, source #7 means editing a growing `if/elif` inside a Spark job and the 30-minute
claim quietly stops being true on the silver side.

- [x] `parse/models.py` — `ParsedItem`, `ParsedComment`, `ParseResult`. `ParsedItem` carries
      its own `parse_error` so one malformed `<item>` in a 20-entry feed quarantines that
      entry, not the other 19; `ParseResult.error` is reserved for the row failing to parse
      at all
- [x] `parse/feedparse.py` — one RSS 2.0 + Atom walker on stdlib `xml.etree.ElementTree`,
      shared directly by `rss_tech`/`rss_verge`/`rss_ars` (`parse/rss.py`) and for its
      primitives by `edgar`/`edgar_formd`. **Parses from bytes, never str** — confirmed
      against real `ISO-8859-1`-declared EDGAR bytes (`test_edgar_iso_8859_1_declaration_
      parses_from_bytes`); a UTF-8-first decode would have mojibake'd or raised before
      parsing even started. On total failure, falls back to `errors="replace"` and flags it
      in `ParseResult.warnings` rather than raising
- [x] `parse/hackernews.py` — `type in (story, job)` → `ParsedItem`; `comment` →
      `ParsedComment`; a self-post's missing `url` falls back to the HN discussion link
      rather than being treated as malformed; an unrecognized type (`poll`/`pollopt`) is
      flagged in `warnings`, not silently absorbed
- [x] `parse/edgar.py` — form type off `<category term=...>`, CIK and filer name off the
      title (`"D - Klondike Holdings LLC (0002144150) (Filer)"`). One parser registered
      for both `edgar` and `edgar_formd` — `type=D` only narrows the feed, the entry shape
      is identical
- [x] `parse/fake.py` — the Phase 0 shape is now just another registry entry; `skeleton.py`
      routes `fake` through `get_parser` + `transform.to_article` on both the Spark and
      in-process paths, so a break here fails `make skeleton` too, not only a parser test
- [x] Fixtures captured from real bronze via `staging.sync_staging` against the live S3
      bucket (all six sources had staged objects), not hand-written — `tests/fixtures/
      bronze/<source>/*`. The one exception is `hackernews/job.json`: no real `type=="job"`
      item existed in the captured window, so that fixture is hand-built to HN's documented
      shape and the test docstring says so

**The date bug, fixed.** The old `transform._parse_published` used `datetime.fromisoformat`
for everything, which cannot read RSS 2.0's RFC 822 `pubDate` (`Tue, 18 Aug 2026 22:32:46
+0000` — TechCrunch and Ars both emit this) and silently returned `None` with
`timestamp_flagged=True`, indistinguishable from §6.2's honest distrust. `feedparse.py` now
uses `email.utils.parsedate_to_datetime` for RSS and keeps `fromisoformat` for Atom's RFC
3339 (`2026-08-18T16:01:01-04:00`), which it always handled correctly.
`test_rss_tech_every_item_parses_without_error` asserts every real captured TechCrunch item
gets a non-`None` `published_at`, so a regression here fails a test, not just a vibe.

**`transform.py` refactored as planned.** `normalize_document` (one JSON blob in, one silver
row out, hardcoded to the fake shape) is now `to_article(parsed: ParsedItem, bronze_row)` —
source-agnostic, unit-tested without a JVM exactly as before. `canonical_url`,
`publisher_domain`, and the hashing/timestamp-disagreement calls are unchanged.

**`spark/jobs/normalize.py` updated, not rewritten.** 2.C owns the real rewrite (Iceberg
sink, MERGE, `silver.hn_comments`, `silver.parse_rejects`) — this only had to stop the job
assuming every bronze row is one fake-shaped JSON article. `mapInPandas` already supports a
row count that differs from its input partition, so `_normalize_row` now calls
`get_parser(source_id)` and fans one bronze row out to zero or more silver rows. A row-level
parse failure still produces exactly one quarantined row (no dedicated reject sink yet —
that's 2.C's `silver.parse_rejects`). `ParseResult.comments` (Hacker News) is intentionally
not emitted by this pass; 2.C's second pass owns `silver.hn_comments`.

**Verification, both skeleton paths, real JVM for the Spark one:**

```
make skeleton-nospark   # 11 raw documents -> 11 articles (2 unverified timestamps) -> 7 clusters (1 exact dupe)
make skeleton           # identical numbers, through Spark's mapInPandas and a real JVM
```

Identical output on both paths is the regression check the runbook's plan called for:
`fake` now goes through the same `get_parser`/`to_article` path real sources do, on both
transports. `make test` (27 new parser tests + the migrated dedup/transform tests),
`make lint`, and `mypy` all pass.

## 2.C — `silver.articles` and `silver.hn_comments` on Iceberg *(done 2026-08-19)*

`spark/jobs/normalize.py` now has two entry points, not one, per CLAUDE.md's "two bronze
paths, don't conflate them": `normalize()` still reads the Phase 0 skeleton's local Parquet
layout directly (no Iceberg, no network — `make skeleton` has to run on a fresh clone with
nothing but PyPI), and `normalize_window()` / `normalize_hn_comments_window()` are the real
job, modeled on `commit_bronze.py`'s shape as planned.

- [x] `normalize_window()`: `ensure_table` + windowed read + MERGE + `NormalizeResult`.
      `_bronze_window` filters on **both** `ingest_date` and `fetched_at` — `ingest_date` is
      the stored column Iceberg can actually prune on, so a `fetched_at`-only predicate would
      silently scan all of bronze (§10.3)
- [x] MERGE on `article_id`, `WHEN NOT MATCHED THEN INSERT` only. `dropDuplicates` before the
      MERGE, same reasoning as `commit_bronze.commit`'s `ingest_id` dedup: an overlapping
      backfill window re-parsing the same feed entry twice must not trip a MERGE cardinality
      error
- [x] `silver.parse_rejects` — one row per bronze row that failed to parse *entirely*
      (`ParseResult.error`), not per malformed entry inside an otherwise-good feed, which
      stays a `parse_error` on its own `silver.articles` row exactly as before. No payload
      column, per plan
- [x] `silver.hn_comments`, second pass over the `hackernews` partitions only. **`story_id`
      resolution turned out to be tractable without touching `silver.articles` at all**: HN
      never returns a story through the same item-JSON walk that returns comments (stories
      become `ParsedItem`s, comments become `ParsedComment`s — `parse/hackernews.py`), so the
      id where a `parent_id` chain stops being a *known comment* is the story, full stop.
      `_resolve_story_ids` walks that chain up to 25 hops against `{already-committed
      silver.hn_comments} ∪ {this batch}`, checkpointing every 5 hops (see below). An
      ancestor never ingested leaves `story_id` pointing at the highest known ancestor rather
      than the true root — a real, documented limit of resolving from single-fetch data, not
      silently papered over
- [x] Deleted the duplicate `build_session` in `normalize.py`. The Iceberg path imports
      `build_iceberg_session` from `spark/session.py`; the skeleton path now imports the
      plain `build_session` from the same module instead of a private copy — `skeleton.py`
      updated accordingly, still no Maven/network touched
- [x] **ADR-0007** — `days(event_date)` where `event_date = coalesce(published_at,
      fetched_at)`, computed in `transform.to_article` so both entry points agree. The
      bytes-scanned measurement is explicitly deferred inside the ADR to 2.F, once Athena
      exists to produce a real number rather than an asserted one (SPEC §17)

### What broke on first real use

- **`Row.count` shadows a column literally named `count`.** `windowed.groupBy("outcome")
  .count()` produces a column named `count`; `Row` subclasses `tuple`, so `row.count` resolves
  to `tuple.count` (a bound method) instead of the value, and the real column is invisible to
  attribute access. Fixed by aliasing to `n` — the same pattern `test_commit_bronze.py`'s raw
  SQL already used (`count(*) n`), just needed at the DataFrame-API call site too.
- **`simhash` has been silently wrong since 2.B.** `hashing.simhash64` returns an unsigned
  64-bit value (up to `2**64 - 1`); `SILVER_SCHEMA` declares `simhash long` (signed). Real
  article text overflows the signed range often enough to make pyarrow's safe cast raise —
  `make skeleton`'s Spark path had simply never hit it, because the Phase 0 fixture's fixed
  text happened to hash under the signed max every time. `_to_signed_i64` reinterprets the
  same bit pattern as two's complement before it crosses into Spark; lossless, because
  `dedup.hamming` XORs-and-masks rather than compares magnitude, proven directly in
  `test_normalize_helpers.py`.
- **`MERGE ... WHEN NOT MATCHED THEN INSERT *` sourced from a `mapInPandas`-rooted
  DataFrame can produce a plan Catalyst's own validator rejects** —
  `PLAN_VALIDATION_FAILED_RULE_IN_BATCH` on `CollapseProject`, "previously resolved and now
  became unresolved" — even with zero rows on either side. `.localCheckpoint(eager=True)`
  right before each `createOrReplaceTempView` breaks the lineage chain the optimizer was
  mis-collapsing. Applied to the articles MERGE source, the rejects MERGE source, and (every
  5 hops, not every hop — 25 checkpoints for a 2-3 level comment thread would be wasteful)
  inside `_resolve_story_ids`'s join loop.
- **`MERGE ... INSERT *` is positional, not by name.** `_resolve_story_ids` appends `story_id`
  as the *last* column; `HN_COMMENTS_DDL` has it third. An explicit `.select(...)` in DDL
  column order fixed it — same reasoning `commit_bronze.read_staged` already applied, just
  not yet followed here before it was tested against a real MERGE.

### Verified

`tests/test_normalize_window.py` (13 tests, `spark`-marked): real bronze rows via
`staging.write_staging` + `commit_bronze.commit` — never hand-assembled table rows — built
from the same real feed fixtures 2.B captured (`rss_tech`, `rss_ars`, `rss_verge`, `edgar`'s
ISO-8859-1 bytes). Covers: articles committed matching the parser's real item count; replay
committing nothing new; `error`/`empty` rows counted but not parsed; a totally malformed
payload landing in `parse_rejects` and not `articles`; window filtering; `event_date`
partitioning; HN comments extracted and kept out of `articles`; `story_id` resolved within
one batch, across two committed batches, and left at the nearest known ancestor when the
true root was never ingested. `make test` (full suite), `make lint`, `mypy`, and both
`make skeleton` / `make skeleton-nospark` (11 raw → 11 articles → 7 clusters, identical on
both paths from a clean `data/`) all pass.

## 2.D — Glue, Athena, and the cost record *(done 2026-08-19, applied)*

- [x] `infra/terraform/main/query.tf` — `silver` and `ops` Glue databases declared (both were
      conjured by Iceberg's `CREATE NAMESPACE IF NOT EXISTS` under the admin identity until
      now — it worked, and was untagged and invisible in state); Athena results bucket
      (`signal-athena-results-481879233905`, 7-day expiry, no `prevent_destroy` — query
      scratch, not the record); `signal` workgroup with `enforce_workgroup_configuration` and
      **`bytes_scanned_cutoff_per_query = 100 MB`** — the guardrail that makes a $5 budget
      survive a `SELECT *` against a table of raw payloads
- [x] `signal-analyst` IAM role, assumable by the admin IAM user (ADR-0005) via `sts:AssumeRole`
      rather than always querying with the admin key — SPEC §17. Scoped to the `signal`
      workgroup, read on the `bronze`/`silver`/`ops` Glue databases, S3 read on bronze's
      warehouse prefix, S3 read/write on the results bucket
- [x] `ops/athena.py` — `run_query` polls `GetQueryExecution` to a terminal state, and
      `athena_cost_usd` **floors at Athena's real 10 MB minimum per query**, so the number
      printed is one the actual bill would match, never an unrounded byte count in disguise
      (SPEC §17)
- [x] `spark/jobs/cost_snapshot.py` — `ops.pipeline_costs`, modeled on `health_snapshot.py`:
      MERGE keyed on `(run_id, dag_id, task_id)`, `UPDATE SET *` on match. Every numeric
      column is optional — a task that only measured egress isn't forced to fabricate
      `bytes_scanned` just to write a row
- [x] `SyncResult.bytes_downloaded` wired into `ops.pipeline_costs` as `s3_egress_bytes` from
      `ingest_monitor_dag.py`'s `commit_staged` task — it was already measured and previously
      died in an XCom the next task explicitly discarded (`del commit_stats`). `s3_requests`
      deliberately left unset: `SyncResult` counts objects considered, not S3 API calls made,
      and claiming the wrong metric under the right name is exactly what §17 rules out
- [x] `local.bronze_prefix` → `local.warehouse_prefix` (value unchanged, `"bronze"`) — the
      warehouse root now holds `bronze.db`, `ops.db`, and `silver.db`
- [x] `signal athena-query --sql "..."` / `make athena-query Q="..."` — prints rows, MB
      scanned, cost, engine time. The acceptance test becomes a command, not a screenshot
- [x] `tests/test_athena.py` (9 tests) — a hand-built fake Athena client for state
      transitions `moto`'s control-plane-only simulation can't produce (`FAILED`,
      multi-poll `RUNNING`→terminal, timeout, real multi-row/multi-column results), plus one
      test against real `moto` proving the actual boto3 call shapes (pagination included)
      are right. `tests/test_cost_snapshot.py` (5 tests, `spark`-marked) — MERGE upsert
      semantics, optional fields staying `NULL`, `months(run_date)` partitioning
- [x] `terraform fmt` + `validate` pass; `terraform plan` against the real account:
      **9 to add, 0 to destroy** (2 Glue databases, the results bucket + 3 sub-resources, the
      workgroup, the IAM role + policy). 6 Lambda functions show a `source_code_hash` change
      — unrelated to this file, picked up from 2.B/2.C's `src/signal_core` changes riding in
      the same deployment zip

### Applied, with one wrinkle

`terraform apply` hit exactly the case `query.tf`'s own comment predicted: `aws_glue_catalog_database.ops` failed with `AlreadyExistsException` — the `ops` namespace was already real, conjured months earlier by Iceberg's `CREATE NAMESPACE IF NOT EXISTS` when `health_snapshot.py` first ran against real AWS (Phase 1), and Terraform had never heard of it. `terraform import aws_glue_catalog_database.ops 481879233905:ops` brought it under management; a second `plan` then showed the honest diff — Terraform wanted to add the `description` and `project=signal` tags and drop the stray `owner=default` parameter Iceberg's auto-create had left — applied cleanly. **9 added, 1 imported, 1 changed on import, 0 destroyed.** `silver` (genuinely new) created without incident. A second `terraform plan` afterward reports no drift.

### The analyst role, verified as a role, not asserted

`aws sts assume-role` into `signal-analyst`, confirmed via `aws sts get-caller-identity`
(`assumed-role/signal-analyst/athena-verify`, not `user/Souhail_Signal_Admin`), then
`signal athena-query` run under those temporary credentials — proving the IAM policy
actually grants what it's supposed to (Athena on the `signal` workgroup, Glue read on
`bronze`, S3 read on bronze's data, S3 read/write on the results bucket) rather than
just parsing. First real question, real AWS, real numbers:

```sql
SELECT source_id, outcome, count(*) AS n FROM raw_documents GROUP BY source_id, outcome
```

8 rows back (`hackernews` 8068 ok / 3 empty, `edgar` 56 ok / 4 error, `edgar_formd` 2,
`rss_tech` 19, `rss_verge` 2, `rss_ars` 1 — matching what 2.B's fixture capture already
showed). **1,969 bytes actually scanned** — column pruning skips `payload` entirely for
a `source_id, outcome` projection — **floored to the 10 MB minimum, $0.0000477**, 2591 ms
engine time. This is against `bronze.raw_documents`, the only table with real data on AWS
right now; `silver.articles` exists on Glue (created by this apply's `ensure_tables`
path when 2.E first runs it for real) but is still empty until then.

**ADR-0007's bytes-scanned comparison and 2.F's three-way table both still need real
`silver.articles` data** — this query proves the Athena/Glue/IAM mechanism end to end,
it isn't the partitioning measurement itself. That's next: 2.E populates silver for
real, then 2.F runs the same-question-three-ways comparison this ADR is waiting on.

## 2.E — The process DAG *(done 2026-08-19, run for real)*

- [x] `SOURCE_IDS` no longer hardcoded — `config.DEPLOYED_SOURCE_IDS` is derived from `SOURCES`
      minus `fake`, and `tests/test_source_registry.py` asserts three-way parity between
      `SOURCES`, `REGISTRY`, and Terraform's `var.sources`. A source declared in two of the
      three places used to fail only at runtime, or never
- [x] `airflow/dags/assets.py` — `BRONZE_COMMITTED`, one `Asset` shared by both DAGs so a
      typo in the URI is a Python error, not a DAG that quietly never triggers.
      `ingest_monitor_dag.py`'s `commit_staged` gets `outlets=[BRONZE_COMMITTED]`;
      `process_dag.py` schedules on `[BRONZE_COMMITTED]` — an Asset event, not a second cron
      hoping the commit already finished. Verified against the real `apache-airflow-task-sdk`
      package (`from airflow.sdk import Asset, dag, task`) before wiring it in, not assumed
      from memory of the Airflow 2 `Dataset` API it replaced
- [x] `airflow/dags/process_dag.py` — two independent tasks, `normalize_articles` and
      `normalize_hn_comments`, both calling `ops.monitor.window_bounds()` for the same
      closed-hour window `ingest_monitor` already uses, so the two tasks never quietly
      disagree on what "this run" covers
- [x] While here: fixed `ingest_monitor_dag.py`'s `get_current_context` import
      (`airflow.operators.python` → `airflow.sdk`, the pre-existing import was already
      deprecated in 3.0.2, caught before it ever ran for real)

### Run for real, end to end

`docker compose up -d --force-recreate` (see below for why), `airflow dags unpause process`,
then `airflow dags trigger ingest_monitor`. `commit_staged` succeeded and emitted the
`BRONZE_COMMITTED` asset event; `process` fired automatically
(`asset_triggered__2026-08-19T11:52:53...`) with no manual trigger — the mechanism 1.C's
runbook entry didn't have a chance to prove, because 2.E is what it was built for.

**Real numbers, one production window:**

| | |
|---|---|
| `normalize_articles` | 521 bronze rows in, 0 skipped, **207 articles committed**, 19 quarantined |
| `normalize_hn_comments` | 511 hackernews bronze rows, 445 comments extracted and committed |
| `ops.pipeline_costs` | `ingest_monitor.commit_staged` — 3,468,248 bytes (3.47 MB) S3 egress, now persisted instead of dying in an XCom |

Checked *why* it rejected 19, not just that it did — the entire spread is one reason:
`SELECT source_id, parse_error, count(*) FROM silver.parse_rejects GROUP BY 1, 2` →
`hackernews, missing_title, 19`. Dead/deleted HN stories, exactly the case
`test_hackernews_dead_story_has_no_title_and_is_quarantined_not_dropped` (2.B) already
covers — quarantined per SPEC §6.2, not a bug. Real `silver.hn_comments` rows show
`story_id` resolving correctly (`item_id=49359311, parent_id=49359236, story_id=49359236`
— a top-level reply, chain terminates in one hop, matching the fixture-tested logic).

### What broke on first real use

**Deleting `.cache/` on the host while Airflow's containers are up breaks their bind
mount, permanently, until they're recreated.** `ingest_monitor` had been running
successfully for hours before this session's 2.B/2.C/2.D work started; every `rm -rf
data out .cache` run to reset the skeleton between test runs was also deleting the
`.cache` directory `docker-compose.yml` bind-mounts into every Airflow container
(`./.cache:/opt/signal/.cache`). The next two scheduled `ingest_monitor` runs failed
with `FileNotFoundError: /opt/signal/.cache/staging` — not a code bug, a bind mount whose
host-side target had been removed out from under a running container. Recreating the
directory on the host was not enough (Docker resolves a bind mount at container *create*
time, not per-access); `docker compose up -d --force-recreate airflow-scheduler
airflow-apiserver airflow-dag-processor` was. Worth a line in `docs/how-signal-works.md`
or a Makefile comment before this repeats: `make clean` and a running `make up` don't mix.

## 2.F — Acceptance: the stranger's question *(done 2026-08-19)* — Phase 2 exit condition met

- [x] `docs/athena.md` — the workgroup, setup (including assuming `signal-analyst`, not
      querying with the admin key — SPEC §17), and four real questions with real answers
      against the live lake
- [x] **The measurement ADR-0007 was waiting on, with real data.** Backfilled
      `normalize_window`/`normalize_hn_comments_window` over all of bronze's currently
      available range (2026-08-18 through 2026-08-19) directly against the deployed
      warehouse — not just the one hour 2.E's live run covered — specifically so the
      partitioning comparison would have more than one calendar day to prune against:
      **9,591 bronze rows → 1,845 new articles (2,052 total), 253 new rejects (272 total)**;
      **9,480 hackernews rows → 8,568 comments extracted.** `silver.articles` now spans
      four real day-partitions (2026-08-10, 17, 18, 19 — EDGAR filings carry real
      historical `published_at` dates, not just "now"). Same question, three ways, against
      `silver.articles`' 1,106 rows for 2026-08-18: `SELECT *` filtered on `published_at`
      (184,259 bytes) → projected columns, same filter (73,373, −60%) → projected +
      filtered on `event_date`, the actual partition column (64,713, another −12%).
      Documented in full, including the two things not to gloss over (cost floors
      identically at this scale; the correctness argument for `event_date` over
      `published_at` has nothing to demonstrate *yet* because no live source's dates are
      null today), in `docs/athena.md` and ADR-0007
- [x] Walked the stranger path **within this checkout** — `make setup`, both skeleton
      paths, `make test` (175 passed), `make lint`, `make tf-validate` from a clean
      `data/`/`out/`/`.cache/staging`. **Not from a literal fresh `git clone`**: none of
      Phase 2's work was committed at the time, and committing is the user's call, not
      made here unprompted.
- [x] **Re-walked from a literal fresh clone, 2026-08-20**, once PR #1 was merged — the
      half of this acceptance test that had to wait for a commit to exist. Cloned the
      merged `main` into a scratch directory and ran the stranger's sequence against a
      brand-new venv: `make setup`, `make skeleton` (real JVM) and `make skeleton-nospark`
      (**11 raw documents → 11 articles → 7 clusters, byte-identical on both paths**),
      `make test` (**175 passed**), `make lint`, `make eval` (all gates passed). Nothing
      in the clone needed a step the README does not list, which is the actual claim this
      test makes. Every `docs/athena.md` command was run verbatim against the real,
      already-applied infrastructure, which is how its first bug was caught: the setup
      example queried `bronze.raw_documents` without `--database bronze` against a CLI
      that defaults to `silver` — `TABLE_NOT_FOUND`, live, on the exact command a stranger
      would have copy-pasted. Fixed (`make athena-query` gained `DB=`), and re-verified
- [x] README: status line (Phase 1 → **Phase 2, done**), the "Measured, not claimed"
      table gained real Phase 2 rows (ingestion window, the three-way Athena numbers, S3
      egress) instead of blank "Phase 4" placeholders for the pieces that are now real,
      and "Adding a source" gained the step 2.B added (`parse/REGISTRY`) that the
      original three-step version silently didn't cover. `how-signal-works.md`'s Phase 2
      section and status table updated to "done", in the same plain-English register as
      the rest of the document
- [x] **Found and fixed a real gap while writing the README's "Adding a source" step 3**:
      `tests/test_source_registry.py` asserted three-way parity (`SOURCES` /
      `sources.REGISTRY` / Terraform) but never checked `parse.REGISTRY` — a source fully
      wired for ingestion could still fail silently the first time `normalize_window` ran,
      exactly the "fails only when it runs" failure mode this test file exists to prevent
      on the ingest side. Added `test_every_configured_source_has_a_parser`

## Open findings

### Stale-but-successful feeds are still undetectable *(found 2026-08-19 — **fixed 2026-08-20**, see `phase-1.md` 1.E)*

**Resolved.** `State.last_content_change_at` now carries the signal, and `assess_source`
reports `dead_feed` against a separate, much longer per-source SLA. The investigation that
closed it found three more layers of the same hole — Hacker News's floor of 0, the absent
volume anomaly detection, and `thin` never failing the DAG — all recorded in 1.E. The
original diagnosis below stands as written; it was correct and understated the scope.

<details>
<summary>Original finding, kept for the trail</summary>


`ops/health.py::assess_source` computes staleness from `State.last_success_at`, and
`feed.poll_feed` advances that on **every** successful fetch — including a 304, and including a
200 whose body has not changed since the last poll. Its own docstring says *"the common failure
is a feed returning 200 with content that has not moved, not a 500... A source can be succeeding
and still be dead"* (SPEC §11), and the implementation cannot currently see that case for any of
the six sources.

`rss_ars` demonstrates it live rather than theoretically: it 304s, `last_success_at` advances,
`staleness_seconds` stays near zero. If Ars froze permanently every poll would look healthy.

The fix is small and contained — track `last_content_change_at`, set only when the fetched
`content_hash` differs from the last one stored, and measure staleness from that instead. It
needs two new `State` fields and one change in `feed.poll_feed`. **Not scheduled yet**; it is a
Phase 1 monitoring gap that Phase 2 doubles the exposure of, and it should be fixed before the
brief's health footer starts making freshness claims in Phase 4.

</details>

*The shape of that fix was right, and one detail of it was wrong: "measure staleness from
that instead" would have **replaced** the fetch clock, losing the ability to detect a
broken poller. Both clocks are kept — they answer different questions, and `dead_feed` is
a separate status from `stale` for the same reason.*

### HN score velocity is structurally unavailable *(found 2026-08-19)*

`sources/hackernews.py` walks item ids **forward** from a watermark, so every item is fetched
exactly once — at creation, when its score is 1 and it has no comments. SPEC §3 says "snapshot
it, never overwrite" and §7.4 wants score slope, but there are no second snapshots to slope
against.

This is why `silver.hn_comments` earns its place: **comment arrival rate per story is derivable
from single-fetch data, and score is not.** A real fix means a second HN poller over
`topstories.json` that re-fetches and snapshots current scores — a source-design change, so
Phase 4, and it has to land before §7.4's velocity component can honestly be claimed.

## Then

**Phase 3 (SPEC §12), reshaped by ADR-0008 after this phase closed.** Two changes to what the
original plan said comes next:

1. **3.0 is a real brief, before any clustering work.** Point `brief/render.py` at real
   `silver.articles` instead of the skeleton's local Parquet — Phase 0's ranker unchanged
   (recency + breadth), no enrichment, no email. It will be a bad brief. It starts the
   daily-reading clock §1 measures success by, two phases earlier than the original plan,
   and it costs almost nothing: the ranker, renderer and template have all been running
   since Phase 0 against fake data. Everything after it improves a page already being read.
2. **Labeling starts immediately and runs alongside the build**, against the real articles
   this phase produced — not in one block once the clusterer is done. ~200 pairs + ~300
   mentions is ~500 judgement calls; at ~20/day it lands with the code, and labeling before
   writing the matcher keeps the labels honest.

Then the original scope: Spark dedup, clustering, and entity resolution, with both labeled
eval sets committed and precision/recall reproducible via `make eval`.

**Both open findings above are now scheduled**, in 4A rather than floating — stale-feed
detection and the HN score-velocity poller are named in SPEC §12's carried-forward table
alongside Phase 1's 1.D, each against the claim it gates.
