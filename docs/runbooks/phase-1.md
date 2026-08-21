# Phase 1 runbook — ingest

Exit condition (SPEC §12): three Lambda pollers on schedule, `bronze.raw_documents` on
Iceberg, local Airflow coordinating monitoring and recovery, and the acceptance test —
stop ingestion for a day, restart, and show that **replay** reprocesses the stored interval
with no duplicates and no gaps while **catch-up** recovers only what each source's backfill
horizon allows and records `gap_reason` for the rest.

## 1.A — Pollers and the bronze table *(done)*
- [x] `hackernews` — watermark walk over sequential item ids, capped at 200 per invocation;
      backfill horizon COMPLETE, so a backlog spreads over later polls and loses nothing
- [x] `edgar` and `rss_tech` — conditional GET (`If-None-Match` / `If-Modified-Since`)
      through the shared `sources/feed.py`; a 304 is a healthy poll with zero documents
- [x] Failures return as `outcome=ERROR` documents, never exceptions — an escaped
      exception means infrastructure, which is what the CloudWatch alarm is for
- [x] `State` in DynamoDB, one item per source (`state_store.py`)
- [x] Payloads staged as gzipped JSONL, committed to Iceberg by Spark — ADR-0006 for why
      Parquet is not written in the Lambda
- [x] `bronze.raw_documents`, partitioned by `(source_id, ingest_date)` (SPEC §6.4);
      commit is a MERGE on `ingest_id`, which is what makes replay safe to re-run
- [x] Acceptance test, both halves: `tests/test_replay_catchup.py`

## 1.B — Infrastructure *(applied 2026-08-18)*
- [x] `infra/terraform/main` — S3 bronze bucket (SSE, public access blocked, staging
      expiry at 14 days), DynamoDB state table, Glue database `bronze`, three Lambdas via
      `for_each` over `var.sources`, EventBridge Scheduler schedules, log groups with
      14-day retention, SNS alerts, and three alarms per source
- [x] `make lambda-package` — 11 MB artifact: httpx, pydantic, pydantic-settings, and the
      source tree. `tests/test_lambda_artifact.py` fails if the handler's import chain
      ever acquires pyarrow, pyspark, pandas, numpy, or jinja2
- [x] `terraform apply` — 30 resources in account 481879233905. Bucket
      `signal-bronze-481879233905`, table `signal-pipeline-state`, functions
      `signal-poll-{hackernews,edgar,rss_tech}`. Everything is inside an always-free tier
      at these cadences (Lambda 1M requests, DynamoDB 25 GB, EventBridge Scheduler 14M
      invocations, Glue's first 1M objects); S3 storage/requests and egress to the local
      Spark box are the only real line items, and 0.C's $5/$20 budgets watch them
- [x] **Reserved concurrency had to go.** The intent was 1 per function; a new account's
      total concurrency limit is 10 and AWS refuses any reservation that takes unreserved
      capacity below 10, so `PutFunctionConcurrency` fails outright. `var.poller_reserved_concurrency`
      defaults to -1. To restore the intent: raise Service Quota **L-B99A9384**, then set
      the variable to 1. Until then every cadence is far longer than its function timeout
      and the scheduler's retry window is shorter than the gap to the next tick
- [x] **SNS subscription confirmed.** Terraform creates it in `PendingConfirmation` and
      cannot tell the difference — an unconfirmed subscription means every alarm above
      fires into nothing, the same trap as 0.C's budget alerts. Verified rather than
      assumed: `aws sns get-subscription-attributes --subscription-arn <arn>` reports
      `PendingConfirmation: false`. Re-check this after any change that recreates the
      subscription
- [x] Activate `project` as a cost-allocation tag — **done 2026-08-20**, exactly as the
      note below predicted: it surfaced as `Inactive` once AWS had processed a period in
      which the ingest resources cost something, and one call activated it. The original
      diagnosis is kept because the failure mode is worth remembering — nothing was broken
      in this repo, and the fix was waiting rather than changing anything. The tag itself
      was fine throughout: `get-tag-keys` returned `environment`, `managed_by`, `project`,
      and 21 resources carried `project=signal`, so Terraform's `default_tags` was working.
      `list-cost-allocation-tags` reads *billing data*, not the tagging API. Commands:

      ```bash
      aws ce list-cost-allocation-tags                       # expect project, Inactive
      aws ce update-cost-allocation-tags-status --cost-allocation-tags-status TagKey=project,Status=Active
      ```

      Discovery is AWS-side and cannot be forced; there is nothing to fix in this repo
      while it is empty. SPEC §10.3 wants cost as a first-class metric, and this tag is
      what makes "what did ingestion cost?" answerable per project rather than per
      account.

### What the first live invocations found

Both failures below were caught by the quarantine path rather than by a crash — they are
`outcome=ERROR` rows sitting in `bronze.raw_documents` right now, which is the design
working (SPEC §6.2).

- **EDGAR 403, "Your Request Originates from an Undeclared Automated Tool."** SEC rejects
  the conventional browser-shaped User-Agent. Measured against the live endpoint: the form
  with a URL in parentheses gets 403, plain `Signal Brief <email>` gets 200. There is now
  one User-Agent in the format the strictest source demands, and a test that fails if
  anyone reintroduces the parentheses.
- **EDGAR read timeout at 10s.** `browse-edgar` is a CGI script, not a static file, and it
  regularly takes longer than that from Lambda. Timeouts are per-source now: 30s for
  EDGAR, 5s for Hacker News (one poll is up to 200 requests, so a generous per-request
  timeout multiplies into a killed invocation).

## 1.C — Orchestration
- [x] `airflow/dags/ingest_monitor_dag.py` — hourly: sync staging to the local read-once
      cache, commit it, assess each source over the hour that just closed, write
      `ops.source_health`, and fail the run when a source is stale or gapped
- [x] The local half run by hand against real AWS, 2026-08-18: 9 staged objects,
      **54 KB egress**, **152 rows** into `bronze.raw_documents` on Glue + S3
      (147 hackernews, 1 rss_tech, 1 edgar, 3 edgar errors). Second pass: **0 bytes
      downloaded, 0 rows committed, 152 recognised as duplicates** — the read-once cache
      and the MERGE both holding
- [x] `make up`, unpause `ingest_monitor`, run it. Verified 2026-08-18: 23 staged objects
      seen, **14 downloaded (239 KB egress) and 9 skipped** — already in the shared cache
      from an earlier hand-run — **851 staged rows, 699 committed, 152 recognised as
      duplicates**, and one `ops.source_health` row per source for the 18:00 window, all
      `ok`. Both idempotence claims held in production, not just in tests

**Compose runs from inside WSL, not Windows.** Three things had to be right, and each
failed in a way that pointed somewhere unhelpful:

- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`. Airflow 3 runs tasks against the Task
  Execution API, defaulting to `localhost:8080` — nothing, inside the scheduler container.
  Every task died with `Connection refused` **before it could open a log file**, so the
  symptom was a failed run with an empty log and no traceback.
- `AIRFLOW_UID`. `~/.aws` is `0600` owned by your host user; a container running as uid
  50000 cannot read it, and botocore's report for an unreadable credentials file is
  "unable to locate credentials" — not a permission error. Running as the host uid fixes
  it without loosening anything on the host.
- Bind mounts, not named volumes, for `.cache`. Docker creates a named volume root-owned,
  which the container then cannot write. The bind mount also means host and containers
  share one read-once cache, so a byte either has paid for is a byte the other does not
  (SPEC §10.1) — which is exactly where those 9 skipped objects came from.

**Spark does not read `s3://` here, and that is deliberate.** Doing so would mean
`hadoop-aws` plus a ~500 MB AWS SDK bundle in every session, and — the part that actually
matters — Spark re-reading the prefix on every retry and every widened replay window, each
one billed as internet egress (SPEC §10.1). `staging.sync_staging` mirrors objects to
`.cache/staging` first; staged objects are immutable, so a file already there is already
correct and is never re-downloaded. The Iceberg *warehouse* is on S3, written through
Iceberg's own S3FileIO, which needs no Hadoop filesystem at all.

## 1.E — Monitoring that can actually see the failures it was built for *(done 2026-08-20)*

Found by testing `assess()` against the real `SOURCES` config rather than reading it. Both
of SPEC §11's headline requirements — dead-feed detection and volume anomaly detection —
were non-functional, in four compounding layers:

| Layer | Before |
|---|---|
| A feed returns 200/304 with frozen content | `last_success_at` advances on both → `staleness ≈ 0` → never `stale` |
| Hacker News produces 0 documents in an hour | `min_docs_per_window=0` → **`ok`** |
| An RSS source produces 0 documents | → `thin` |
| `thin` reaches the DAG | not in its hardcoded failure set → **run passes green** |
| Any source drops 80% | → `ok`; no anomaly detection existed |

Measured before the fix, against the deployed config: a **totally silent Hacker News read
`ok`**, and an 80% drop read `ok` on every source. `assess_source`'s own docstring
described the dead-feed capability ("a source can be succeeding and still be dead") that
the function did not have — the inverse of SPEC §17.

- [x] `State.last_content_change_at` + `last_content_hash`. The content-movement signal is
      per-poller because the honest signal differs: `feed.py` compares the body's
      `content_hash`; `hackernews.py` has no body to hash and moves iff the watermark
      advanced. A 304, an unchanged 200, and a failed fetch all leave the clock alone
- [x] `SourceConfig.content_staleness_sla_seconds`, separate from `freshness_sla_seconds`
      and far longer, because they answer different questions — "is the poller working?"
      vs "is the publisher alive?". A single field could not serve both: 45 minutes is
      right for the first and absurd for the second
- [x] **`min_docs_per_window` recalibrated against 41 hours of real production data**, and
      the old values were wrong in both directions. `hackernews` was 0 — commented "a
      quiet minute on HN is normal", true of a minute while the assessment window is an
      *hour* — so the pipeline's highest-volume source could go silent and pass. The three
      RSS sources were 1, but they mostly 304 and legitimately produce **zero** documents
      most hours, so that floor would have fired constantly once `thin` started failing
      runs. Now 50 and 0 respectively
- [x] Volume anomaly detection: `assess_source` takes `baseline_docs`, the DAG supplies it
      as a **median** over the last 24 windows from `ops.source_health`. Median, not mean,
      because an outage's own zero-windows land in that table and a mean lets them drag the
      baseline down until the outage becomes the new normal. Skipped below a baseline of
      10, where percentage arithmetic is noise — those sources are covered by `dead_feed`
- [x] `FAILING_STATUSES` defined once in `ops/health.py` and imported by the DAG. The
      DAG's own literal set is what let `thin` become a status nothing acted on;
      `test_every_non_ok_status_fails_the_dag` now fails if the two drift again
- [x] `ops.source_health` gained `content_staleness_seconds` and `baseline_docs` via an
      idempotent `ALTER TABLE ADD COLUMN` in `ensure_table` — `CREATE TABLE IF NOT EXISTS`
      is a no-op against the live AWS table, so a new DDL column would never have reached
      it and the MERGE would have failed on a column the target lacked. Iceberg schema
      evolution is metadata-only, so existing rows read NULL, which is the truth about
      windows assessed before these signals existed

**A boundary bug the scenario replay caught.** `docs < baseline * 0.2` read a drop to
*exactly* 20% as healthy — so 900/hour falling to 180 passed, which is the single number
most likely to be quoted at the check. §11 asks for a drop *of 80%* to alert, and 20%
remaining is exactly that: `<=`.

Verified by replaying the three scenarios that produced the original finding: frozen
content now reads `dead_feed` on all six sources, an 80% drop reads `volume_drop`, and six
genuinely-healthy shapes (including three RSS sources at zero documents) all still read
`ok`. 190 tests, `make lint`, `mypy`, and both skeleton paths (11 → 7 clusters, identical)
pass.

### Run for real against AWS, 2026-08-20

`ingest_monitor` triggered manually; all three tasks succeeded. The newest
`ops.source_health` window, read back through Athena as `signal-analyst`:

| source_id | status | docs | floor | baseline |
|---|---|---|---|---|
| edgar | ok | 4 | 1 | 4 |
| edgar_formd | ok | 4 | 1 | 4 |
| hackernews | ok | 454 | 50 | 511 |
| rss_ars | ok | 0 | 0 | 0 |
| rss_tech | ok | 0 | 0 | 1 |
| rss_verge | ok | 2 | 0 | 2 |

Three things this confirms that no unit test could. The **schema migration ran against the
live table** — both new columns exist and are populated. The **baselines are real**:
hackernews's median of 511 matches the ~500/hour measured independently from bronze, and
`rss_tech`'s baseline of 1 is correctly below `VOLUME_BASELINE_MIN`, so its volume check is
skipped rather than firing on noise. And **the recalibrated floors do not false-fire**:
`rss_tech` and `rss_ars` both returned zero documents this window and both read `ok` — under
the old `min_docs_per_window=1` they would have been `thin`, which as of this change fails
the DAG. That combination, shipped together, would have produced a red run every quiet hour.

- [x] **`terraform apply` to deploy the new poller code** — done 2026-08-20 11:31, all six
      functions on the new `source_code_hash`, no drift afterwards.

### The state store was silently discarding both new fields *(found and fixed 2026-08-20)*

Caught by checking DynamoDB after the deploy rather than trusting it: `hackernews` had
polled at 11:34 with the new code and `last_content_change_at` was **still unset**.

`state_store._to_item` and `_from_item` enumerate `State`'s fields **explicitly** — a
deliberate design (SPEC §6.2: only write attributes that are set, because DynamoDB has no
Python `None` and would round-trip one back as the string `"None"`). The cost of that
design is that a new field on `State` is dropped in silence. The pollers computed
`last_content_change_at` correctly, the store threw it away on save, and `assess_source`
read `None` forever — so the `dead_feed` check would have been **permanently inert against
a field that was always null**, while every test passed.

The unit tests could not see it: they assert on the `State` object a poller *returns*,
never on what survives a round trip through DynamoDB. The fix is four lines; the guard
against a recurrence is the part worth having —
`test_every_state_field_survives_a_round_trip` populates every field, round-trips it, and
compares `State.model_fields` rather than a hand-written list, so **adding a field to
`State` without teaching the store about it now fails a test**. Verified by reverting the
fix and watching it fail with the field named.

Requires a second `terraform apply` to reach production (plan: 0 add, 6 change, 0 destroy).

## 1.D — The acceptance test, for real

**Prerequisite:** the state-store fix above must be deployed, or the outage runs with
`last_content_change_at` still null and the dead-feed half of the monitoring proves
nothing.

`var.poller_schedule_state` (added 2026-08-20) is how ingestion is stopped and restarted —
one variable, not six `aws scheduler update-schedule` calls. That CLI call is a full
replace, so hand-disabling risks dropping each schedule's retry policy on the way back; and
with the state unmanaged Terraform assumes its `ENABLED` default, so **any** apply during
the outage — even for an unrelated change — would quietly restart ingestion and void a test
that takes a day.

### Step 1 — Capture the before-state

None of the claims below are checkable without it. Record into `docs/runbooks/phase-1.md`
or a scratch file, and **note the UTC time**.

**The two captures need different identities, and must not share a shell.** `signal-analyst`
is scoped to Athena, Glue and S3 (2.D) and has no DynamoDB permission — correctly, because
pipeline state is operational, not analytical. Exporting its credentials and then reading
DynamoDB in the same shell fails with `AccessDeniedException` on `dynamodb:GetItem`. That is
least privilege working; **do not widen the analyst role to make it pass.** Read state with
your admin identity, and keep the analyst credentials inside a subshell so they cannot leak
into the rest of the session.

```bash
date -u +%Y-%m-%dT%H:%M:%SZ                      # T0 — the outage starts here

# --- Pipeline state, as the admin identity. Watermarks are what catch-up is measured
# --- against; last_content_change_at must be set on all six before starting (see below).
for s in hackernews edgar edgar_formd rss_tech rss_verge rss_ars; do
  aws dynamodb get-item --table-name signal-pipeline-state \
    --key "{\"source_id\":{\"S\":\"$s\"}}" --output json \
    --query 'Item.{src:source_id.S,wm:watermark.N,succ:last_success_at.S,content:last_content_change_at.S}'
done

# --- Bronze rows per source, as signal-analyst. Subshell: the exports die with it.
(
  CREDS=$(aws sts assume-role \
    --role-arn "$(terraform -chdir=infra/terraform/main output -raw analyst_role_arn)" \
    --role-session-name 1d-before \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)
  read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN <<< "$CREDS"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

  uv run signal athena-query --database bronze --sql \
    "SELECT source_id, count(*) AS rows, max(fetched_at) AS newest
     FROM raw_documents GROUP BY source_id ORDER BY source_id"
)
```

**Gate before Step 2: every source must have polled once with the deployed code.**
`last_content_change_at` is written by the poller, so a source that has not polled since the
deploy carries `NOT SET` into the outage and its dead-feed signal has no baseline to measure
from. `hackernews` polls every 5 minutes; the other five every 15. Wait until all six show a
value — `hackernews` legitimately shows no `last_content_hash` (it uses the watermark signal,
not a body hash), which is the design, not a gap.

> **This gate is what found the 304 seeding bug.** Waiting on it, five of six sources had
> the field within minutes and `rss_ars` never did: it serves `Last-Modified`, 304s nearly
> every poll, and the 304 branch returned before touching the field. `assess_source` reads
> an unset value as "no content-movement signal, skip the check", so the lowest-volume
> source — the one most likely to die unnoticed — was the single source **exempt from
> dead-feed detection**, indefinitely. A 304 now seeds the field when unset while still
> never advancing it. Worth recording because no test would have caught it: every unit test
> starts from a 200, which is the one path that already worked.

### RUN OF RECORD — T0 = **2026-08-20T12:37:25Z**

Ingestion stopped at 12:37:25Z; all six schedules confirmed `DISABLED` immediately after.

**Pipeline state at T0** — every source healthy, zero consecutive failures, and every
`last_content_change_at` seeded (the gate above, which is what caught the 304 bug):

| source_id | watermark | last_success_at | last_content_change_at |
|---|---|---|---|
| hackernews | 49373752 | 12:34:36 | 12:34:36 |
| edgar | — | 12:29:51 | 12:29:51 |
| edgar_formd | — | 12:27:58 | 12:27:58 |
| rss_tech | — | 12:29:17 | 12:29:17 |
| rss_verge | — | 12:27:23 | 12:27:23 |
| rss_ars | — | 12:27:15 | 12:27:15 |

`hackernews`'s watermark **49373752** is the number catch-up is measured against: COMPLETE
horizon means every id above it must eventually arrive.

**Bronze at T0** (`newest` is each source's last *committed* document):

| source_id | bronze rows | newest committed |
|---|---|---|
| edgar | 168 | 11:59:42 |
| edgar_formd | 110 | 11:57:58 |
| hackernews | 22,593 | 12:04:32 |
| rss_ars | 6 | 00:57:15 |
| rss_tech | 65 | 09:44:17 |
| rss_verge | 54 | 11:57:23 |

> **Expect bronze to keep growing after T0, and do not read that as ingestion still
> running.** Pollers write to S3 staging; `ingest_monitor` commits staging into bronze
> hourly, so the two are always a partial hour apart. At T0 there were **912 staged
> objects (9.7 MB)** not yet committed, the newest from 12:34:37 — polls that happened
> after the last DAG run. They will land in bronze on the next `ingest_monitor` run, *during*
> the outage. Staging is a queue and bronze is the record (1.A); this is that distinction
> showing up in the numbers.
>
> This also means the **replay check in Step 5 must compare against bronze counts taken
> after staging has drained**, not against the table above. Take a fresh count once
> `ingest_monitor` has run at least once post-T0 with `committed_rows == 0`, and use that
> as the replay baseline.

<details>
<summary>Earlier capture at ~11:20 UTC, superseded by the T0 numbers above</summary>

| source_id | bronze rows | newest |
|---|---|---|
| edgar | 165 | 11:14:42 |
| edgar_formd | 107 | 11:13:04 |
| hackernews | 22,233 | 11:19:35 |
| rss_ars | 6 | 00:57:15 |
| rss_tech | 65 | 09:44:17 |
| rss_verge | 51 | 11:12:23 |

</details>

### The analyst credentials leaked into the shell and broke the apply *(2026-08-20)*

The first `terraform apply -var poller_schedule_state=DISABLED` failed:

```
Error acquiring the state lock ... User: arn:aws:sts::...:assumed-role/signal-analyst/1d-before
is not authorized to perform: s3:PutObject on ... main/terraform.tfstate.tflock
```

The capture block's `export` had put `signal-analyst` credentials in the interactive shell,
where they persisted (assume-role sessions last an hour by default) and every later AWS
command silently inherited them — including Terraform, which then could not write its own
state lock. Fixed with `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN`;
the block above now uses a subshell so the credentials die with it.

Two non-fixes worth naming, because both are tempting and both are wrong:

- **`-lock=false`**, which Terraform's own error message suggests. That advice is for a
  genuinely stuck lock. Here the lock was fine and the *identity* was wrong; disabling
  locking would have let a half-privileged apply write state unprotected.
- **Widening `signal-analyst`.** It blocked two different things today — reading DynamoDB
  pipeline state, and writing Terraform state — and was right both times. It exists to
  query the lake. An analyst role that can run Terraform is not an analyst role.

Nothing was changed by the failed apply: it died acquiring the lock before touching any
resource, left no lock object behind (the `PutObject` was denied, so none was created), and
all six schedules were still `ENABLED` afterwards. The habit this argues for: run
`aws sts get-caller-identity` after any `assume-role`, before the next privileged command.

### Step 2 — Stop ingestion

```bash
terraform -chdir=infra/terraform/main apply -var poller_schedule_state=DISABLED
```

Expect **6 to change** (the schedules; more if Lambda code is also pending). Confirm:

```bash
aws scheduler list-schedules --query 'Schedules[*].[Name,State]' --output text
```

**Leave `ingest_monitor` unpaused.** Watching it detect the outage is half the value of
the exercise, and as of 1.E it can actually do that. It will go red hourly — that is the
test passing, not a problem to fix. `commit_staged` keeps succeeding (nothing to commit is
idempotent, not an error); `raise_on_degraded` is what fails.

Also expect **CloudWatch `-not-running` alarm emails** for all six within ~15 minutes.
Same reasoning: the alarm firing during a deliberate outage is the alarm working.

### Step 3 — Watch it degrade

The SLAs say when. `freshness_sla_seconds` is 900s for `hackernews` and 2700s for the other
five, so:

| Elapsed | Expected |
|---|---|
| ~15 min | `hackernews` → `stale` |
| ~45 min | all six → `stale` |
| ~1 h+ | unchanged — `stale` outranks `dead_feed`, correctly: the poller is what's broken, not the publisher |

Read the verdicts back at a few points, and keep at least one for the record:

```bash
uv run signal athena-query --database ops --sql \
  "SELECT source_id, status, docs_ingested,
          cast(staleness_seconds AS bigint) AS stale_s,
          cast(content_staleness_seconds AS bigint) AS content_s
   FROM source_health
   WHERE window_start = (SELECT max(window_start) FROM source_health)
   ORDER BY source_id"
```

### RESULT — Step 3, the degradation

It went exactly as the SLA table predicts, which is the point of writing the prediction
down first. `status` per source, by assessment window:

| window (UTC) | assessed ~ | hackernews | edgar | edgar_formd | rss_tech | rss_verge | rss_ars |
|---|---|---|---|---|---|---|---|
| 08-20 12:00 | 13:05 | **stale** (1,892 s) | ok (2,177 s) | ok (2,290 s) | ok (2,211 s) | ok (2,325 s) | ok (2,333 s) |
| 08-20 13:00 | 14:05 | stale (5,483 s) | **stale** (5,768 s) | **stale** (5,881 s) | **stale** (5,802 s) | **stale** (5,916 s) | **stale** (5,924 s) |
| … through 08-21 11:00 | 12:05 | stale (84,685 s) | stale (84,970 s) | stale (85,083 s) | stale (85,004 s) | stale (85,119 s) | stale (85,126 s) |

`hackernews` goes red one window before everything else — its `freshness_sla_seconds` is
900 against the others' 2,700 — and nothing ever escalates past `stale`. That last part is
the ordering working: `stale` outranks `dead_feed`, and during a poller outage the poller
is what is broken, not the publisher.

`content_staleness_seconds` tracked `staleness_seconds` to within a second all the way
through, because at T0 every source's `last_content_change_at` equalled its
`last_success_at`. That is the 1.E gate from Step 1 paying off: the field was seeded on all
six, including `rss_ars` via the 304 fix, so the dead-feed half of the monitoring had a
real baseline rather than a null.

> **A hole in the record, stated rather than smoothed over.** `source_health` has eleven
> windows for a 24-hour outage: `08-20 12:00`–`20:00`, then nothing until `08-21 10:00`.
> The WSL2 host slept overnight, so `ingest_monitor` did not run between roughly 21:05 and
> 11:28; the two runs that did fire on waking assessed the *current* hour, not the ones
> they missed. The outage itself was unaffected — the schedules live in EventBridge and
> stayed `DISABLED` throughout, and DynamoDB confirms zero polls — but the local monitoring
> layer has a 13-hour blind spot, and the `catchup=False` on this DAG is why it did not
> backfill them. Worth naming because it is the same failure the test exists to catch,
> arriving from the side nobody was watching.

### Step 4 — Restart, after a full day

```bash
terraform -chdir=infra/terraform/main apply    # ENABLED is the default — no -var needed
date -u +%Y-%m-%dT%H:%M:%SZ                    # T1 — record it
```

### RESULT — Step 4, the restart

`plan` first, to confirm nothing unrelated had drifted in during the day:

```
Plan: 0 to add, 6 to change, 0 to destroy.
```

Six `state = "DISABLED" -> "ENABLED"` and nothing else. Worth checking rather than
assuming: an apply that *also* redeploys Lambda code is an apply doing two things at once
in the middle of an acceptance test, and the schedule change would no longer be the only
variable.

| | |
|---|---|
| T0 — ingestion stopped | **2026-08-20T12:37:25Z** |
| T1 — ingestion restarted | **2026-08-21T12:49:15Z** |
| Outage | **24 h 11 m 50 s (24.20 h)** |
| First poll after restart | `hackernews` at 12:50:47Z, 92 s later |

All six schedules read back `ENABLED`.

### Step 5 — Replay: the guarantee that always holds

**There is no `make replay`** despite SPEC §13 listing one. Replay is `ingest_monitor`'s
`commit_staged` task, which MERGEs on `ingest_id` and is therefore idempotent by
construction. Re-run it over the stored interval:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger ingest_monitor
```

Read `commit_staged`'s return value from the Airflow UI (XCom) or the task log. **Pass
condition: `committed_rows == 0` and `table_rows` unchanged from Step 1**, with
`duplicate_rows` equal to whatever was re-read. That is the whole replay claim: reprocessing
a stored interval inserts nothing and loses nothing.

### RESULT — Step 5, replay

**First, the baseline the check is actually against.** Step 1 warned that the T0 bronze
table is the wrong comparison, because 912 staged objects were still uncommitted when the
outage began. The `commit_staged` XCom trail shows exactly that queue draining, and then
nothing:

| `ingest_monitor` run | objects downloaded | egress | `committed_rows` | `duplicate_rows` | `table_rows` |
|---|---|---|---|---|---|
| 2026-08-20 13:05 — first after T0 | 14 | 145,510 B | **310** | 22,996 | 23,306 |
| 14:05 → 2026-08-21 12:05 — twelve runs | 0 | 0 B | **0** | 23,306 | 23,306 |

`duplicate_rows` on the drain run is **22,996**, which is the T0 bronze total exactly
(168 + 110 + 22,593 + 6 + 65 + 54). The queue landed its 310 rows on the first run of the
outage and the table has not moved since.

Per source, T0 vs. post-drain — the latter is the real replay baseline, read at 12:47Z
while ingestion was still off:

| source_id | T0 rows | post-drain rows | Δ | newest committed |
|---|---|---|---|---|
| edgar | 168 | 170 | +2 | 2026-08-20 12:29:51 |
| edgar_formd | 110 | 112 | +2 | 2026-08-20 12:27:58 |
| hackernews | 22,593 | 22,895 | +302 | 2026-08-20 12:34:36 |
| rss_ars | 6 | 6 | 0 | 2026-08-20 00:57:15 |
| rss_tech | 65 | 67 | +2 | 2026-08-20 12:29:17 |
| rss_verge | 54 | 56 | +2 | 2026-08-20 12:27:23 |
| **total** | **22,996** | **23,306** | **+310** | |

Nothing in bronze is newer than T0, and all six DynamoDB state items were still frozen at
their T0 `watermark` / `last_success_at` / `last_content_change_at` values. A full day with
zero writes is what makes the replay number below mean something rather than being
incidentally true.

**The replay run.** Triggered at 12:49:38Z — 23 seconds after the restart and deliberately
ahead of the first poll, so the interval replayed is exactly the stored one:

```json
{"objects_seen": 912, "objects_downloaded": 0, "egress_bytes": 0,
 "staged_rows": 23306, "committed_rows": 0, "duplicate_rows": 23306, "table_rows": 23306}
```

**Pass.** `committed_rows == 0`, `table_rows` unchanged at 23,306, `duplicate_rows` equal
to every row re-read, and `egress_bytes == 0` because the local staging cache served the
re-read without touching S3. Reprocessing a stored interval inserted nothing and lost
nothing — and the twelve idle hourly runs above prove the same thing twelve more times
without anyone having asked them to.

The stronger version of the claim arrived over the next four hours without being asked for
either. Idle re-runs only show that a MERGE over an unchanged table is a no-op. These show
it discriminating correctly on a table that is actively growing, which is the case that
actually happens during a recovery:

| run | `duplicate_rows` | `committed_rows` | `table_rows` |
|---|---|---|---|
| 13:05 | 23,306 | 607 | 23,913 |
| 14:05 | 23,913 | 2,413 | 26,326 |
| 15:05 | 26,326 | 2,414 | 28,740 |
| 16:05 | 28,740 | 2,415 | 31,155 |

Read the invariant down the diagonal: **every run's `duplicate_rows` is exactly the previous
run's `table_rows`.** Each hour the job re-read every row already committed, inserted none of
them, and inserted precisely the new ones — while 2,414 rows an hour were pouring in from the
`hackernews` backlog. That the committed counts land on 2,413 / 2,414 / 2,415 is the poller
hitting its 200-per-5-minute ceiling twelve times an hour without a miss, and it is the
clearest single number in this test: replay and catch-up running simultaneously, neither
corrupting the other.

### Step 6 — Catch-up: the guarantee that does not

**Catch-up is not a command.** `ops/recovery.py::plan_catch_up` *plans* it and writes a
`gap_reason`; the recovery itself is what the pollers do naturally once re-enabled — HN
walks its watermark forward, and an RSS feed simply returns whatever is currently in its
window. This distinction is SPEC §6.3 and the reason the two words are kept apart.

What each source should do, and why:

| Source | Horizon | Expected |
|---|---|---|
| `hackernews` | COMPLETE | Recovers **everything** — but *not immediately*. `MAX_ITEMS_PER_POLL` is 200 and the cadence is 5 minutes, so a day's backlog (~15-20k items) drains at ~2,400/hour and takes **several hours**. A watermark still climbing hours later is the design working, not a stall |
| `edgar`, `edgar_formd` | DAY | Recovers roughly the last day from the current feed, gaps anything older. A 24-hour outage sits right on that boundary — expect partial recovery and a `gap_reason` |
| `rss_tech`, `rss_verge`, `rss_ars` | WINDOW | Recovers **almost nothing** — only what is still in the feed (~1-3 h) — and records a `gap_reason` saying so. **This is the correct outcome, not a bug**, and it is the single most important result of this test |

Let it run for several hours, then record the split:

```bash
uv run signal athena-query --database ops --sql \
  "SELECT source_id, status, gap_reason, window_start FROM source_health
   WHERE gap_reason IS NOT NULL ORDER BY window_start DESC, source_id LIMIT 20"

uv run signal athena-query --database bronze --sql \
  "SELECT source_id, count(*) AS rows FROM raw_documents GROUP BY source_id ORDER BY source_id"
```

### RESULT — Step 6, catch-up

Recovery began 92 seconds after the restart and every source was back to `ok` by the
13:05 `ingest_monitor` run — the first run of the day to pass `raise_on_degraded`, sixteen
minutes after T1. That run committed **607 rows** and took bronze from 23,306 to 23,913.

#### `hackernews` (COMPLETE) — recovers everything, slowly, exactly as specified

The watermark walks forward in perfect 200-item steps, one per 5-minute poll:

| sample (UTC) | watermark | HN `maxitem` | items behind |
|---|---|---|---|
| 12:51:31 | 49,373,952 | 49,387,333 | 13,381 |
| 12:56:32 | 49,374,152 | 49,387,401 | 13,249 |
| 13:01:34 | 49,374,352 | 49,387,458 | 13,106 |
| 13:06:35 | 49,374,552 | 49,387,516 | 12,964 |
| 16:42:40 | 49,383,152 | 49,390,669 | 7,517 |
| 16:47:02 | 49,383,352 | 49,390,733 | 7,381 |

T0's watermark was **49,373,752**, so the backlog at restart was **13,581 items**.
`MAX_ITEMS_PER_POLL` is 200 and the cadence is 5 minutes, giving 2,400 items/hour gross —
and measured over the first 3.9 hours the watermark advanced 9,200, i.e. **2,390/hour**, so
the poller hit its ceiling essentially every single poll with no misses. HN itself produced
867 items/hour over the same window, leaving a **net closure rate of ~1,520 items/hour** and
a projected full drain around **21:30 UTC**, roughly 8.7 hours after the restart.

Nothing here needs supervising — a watermark still climbing at 20:00 is the design working,
and the gap closing at a constant rate is the only evidence needed that it is not stuck.
Confirm with:

```bash
aws dynamodb get-item --table-name signal-pipeline-state \
  --key '{"source_id":{"S":"hackernews"}}' --query 'Item.watermark.N' --output text
curl -s https://hacker-news.firebaseio.com/v0/maxitem.json
```

#### The RSS sources — and the result that did not match the prediction

The table above predicted RSS "recovers **almost nothing** — only what is still in the feed
(~1-3 h)". That is not what happened, and the difference is worth more than the prediction
was.

Measured by parsing each source's **last pre-outage** and **first post-restart** feed
snapshot straight out of `staging/` (no silver involved — this is bronze bytes and the
Phase 2 parsers):

| source | items held | pre-outage items rotated out | oldest item in post-restart snapshot | published during outage, recovered | **genuinely lost** |
|---|---|---|---|---|---|
| `rss_ars` | 20 (fixed) | 10 / 20 | 2026-08-19 15:56:56 | **10** | **0.0 h** |
| `rss_tech` | 20 (fixed) | 20 / 20 | 2026-08-20 16:07:26 | **20** | **3.6 h** (~8 items) |
| `rss_verge` | 10 (fixed) | 10 / 10 | 2026-08-20 17:42:34 | **10** | **5.3 h** (~3 items) |

The **hour figures are measured** — the interval between each source's last successful poll
and the oldest item its feed still carried on the first fetch back. The **item counts are
extrapolations** from each feed's observed publishing rate over its own snapshot span, and
are the softest number here: a source publishing faster during the lost daytime hours than
its 24-hour average would have lost more than the estimate says.

Item counts are identical before and after, which is the whole explanation: **these are
fixed-*count* feeds, not fixed-*duration* windows.** A feed that holds 20 items reaches back
20 items' worth of time, so its horizon in hours is inversely proportional to how fast the
source publishes. Ars Technica publishes ~0.46 items/hour, so 20 slots span ~45 hours and a
24-hour outage costs it *nothing*. The Verge holds only 10 slots and lost 5.3 hours.

**The recorded `gap_reason` was wrong in the safe direction, and by a lot.** The last
pre-restart verdict claimed, verbatim and identically for all three:

> `22.6h unrecovered (window horizon): the feed holds only its current window, so items published during the outage have rotated out and are unrecoverable`

Against a measured loss of 0.0 h, 3.6 h and 5.3 h. `HORIZON_REACH[WINDOW]` is a flat
`timedelta(hours=1)` and `recovery.py` is explicit that this is a deliberate conservative
floor — "claiming three and recovering one is worse than claiming one" — so the gap is an
*upper bound on loss*, never an under-report. That is the right direction for an
operational signal. But it is a bound, not a measurement, and after a real 24-hour outage
the measurement is now available and should be published next to it rather than instead of
it.

**The caveat that keeps this honest, and it is the important half.** Recovery was this good
partly by luck of timing: the outage spanned a quiet overnight period. `rss_tech` published
20 items in the 8.3 hours it *was* active, and at that rate a 20-slot feed reaches back only
~8 hours — a 24-hour outage across a full news day would have lost ~16 of them. A
fixed-count feed's horizon **collapses exactly when the source is busiest**, which is also
when the missed items matter most. The honest claim is therefore not "RSS recovers fine" but
**"RSS loss is rate-dependent, ranged 0–5.3 h here, and is unbounded above in the general
case"**.

#### `edgar` / `edgar_formd` (DAY) — never recorded a gap at all

Neither ever carried a `gap_reason`. `HORIZON_REACH[DAY]` is 24 hours and the outage was
24.20 hours, so the gap existed for only its final 11 minutes — and the outage crossed the
24-hour line at 12:37:25Z, twelve minutes before the restart, with no assessment window in
between to notice. The runbook's "a 24-hour outage sits right on that boundary" was correct;
it landed on the recoverable side by about the width of one poll.

#### A property of `gap_reason` worth knowing before you go looking for it

`assess` derives the outage interval from `last_success_at`, so the moment a source polls
successfully the interval collapses and the `gap_reason` stops being written. By the 13:05
window all six sources read `status = ok`, `gap_reason = NULL`. **The record of what was
lost survives only in the historical `source_health` rows** — query by `window_start` range,
never by "latest window", or the outage will look like it cost nothing.

#### `content_staleness_seconds` after recovery

Populated on all six, and — the part that matters — no longer tracking `staleness_seconds`:

| source | `stale_s` | `content_s` |
|---|---|---|
| hackernews | 43 | 43 |
| edgar | 74 | 74 |
| edgar_formd | 62 | 62 |
| rss_tech | 98 | 98 |
| rss_ars | 105 | **968** |
| rss_verge | 96 | **968** |

`rss_ars` and `rss_verge` polled successfully 105 s and 96 s ago but have not changed
content for 968 s — they 304'd. The two clocks separating is the dead-feed signal doing the
one thing a plain freshness check cannot, which is the deferred half of 1.E reaching
production.

### Step 7 — Write it down

- [x] Record the real numbers here: bronze rows before/after per source, replay's
      `committed_rows`/`table_rows`, and the recovered-vs-lost split with each
      `gap_reason` quoted verbatim — the four `RESULT` sections above, and the
      before/after bronze table below
- [x] Put the recovered/lost split in the README (SPEC §16 item 6). The honest version —
      "RSS loses most of an outage and here is the number" — is the claim worth making;
      the interviewer's question is whether you know what you *cannot* recover.
      README → "Replay and catch-up are different" → "Measured, on the deployed pipeline",
      plus two rows in "Measured, not claimed". The claim that ended up being worth making
      was not the one predicted: RSS loss is **rate-dependent**, measured 0–5.3 h here and
      unbounded above, and the pipeline's own `gap_reason` over-reported it. Both the bound
      and the measurement are published, because publishing only the flattering one would be
      the exact failure this test exists to prevent
- [x] Confirm `content_staleness_seconds` is populated after recovery, which is the
      deferred half of 1.E reaching production — populated on all six, and diverging from
      `staleness_seconds` on `rss_ars`/`rss_verge` (968 s vs ~100 s), which is the signal
      doing something a freshness check cannot

### Bronze, end to end

| source_id | T0 (12:37Z, 08-20) | post-drain (12:47Z, 08-21) | after catch-up (16:47Z, 08-21 — 4.0 h in, still draining) |
|---|---|---|---|
| edgar | 168 | 170 | 184 |
| edgar_formd | 110 | 112 | 125 |
| hackernews | 22,593 | 22,895 | **30,695** |
| rss_ars | 6 | 6 | 8 |
| rss_tech | 65 | 67 | 78 |
| rss_verge | 54 | 56 | 65 |
| **total** | **22,996** | **23,306** | **31,155** |

`hackernews` accounts for 7,800 of the 7,849 rows added since the restart, which is what a
COMPLETE horizon draining a backlog looks like. The RSS sources added 2, 11 and 9 rows —
remember a feed row is one *changed-body fetch*, not one article (a 304 returns no document
at all), so these count how often each feed moved, not what it carried.

**This is a mid-drain snapshot, not a final one.** At 16:47Z `hackernews` was at watermark
49,383,352 against a live `maxitem` of 49,390,733 — still **7,381 items behind**, closing at
~1,520/hour net, projected complete around **21:30 UTC**. The row above will keep climbing
for several hours after this was written, and that is the design working. Re-check with the
two commands in the `hackernews` section above.

## 1.D — The acceptance test, for real *(after apply)*
The test in `tests/test_replay_catchup.py` proves the mechanism against a temp warehouse.
Doing it against the deployed pipeline is the deliverable:

(Superseded by the walkthrough above — kept only as the original sketch.)

1. Disable the three EventBridge schedules. Note the time.
2. Leave it off for a day. The brief's footer should degrade as each source passes its
   freshness SLA — 15 minutes for `hackernews`, 45 for `edgar` and `rss_tech`. Each is 3x
   its deployed cadence: an SLA shorter than the poll interval reports a healthy source as
   permanently stale, which trains the alert away rather than detecting anything. Change
   these and `var.sources` together.
3. Re-enable the schedules.
4. **Replay:** re-run the commit job over the stored interval. It must report
   `committed_rows == 0` and leave `table_rows` unchanged.
5. **Catch-up:** `hackernews` should recover the full day from its watermark.
   `edgar` recovers roughly the last day and gaps the rest. `rss_tech` recovers almost
   nothing and records a `gap_reason` saying so — that is the correct outcome, not a bug.
6. Record the actual numbers here, and put the recovered/lost split in the README.

## Notes
- The pollers' User-Agent carries a contact email because SEC requires it and blocks
  fair-access violators (SPEC §6.2). It comes from `SIGNAL_CONTACT_EMAIL`, set by
  Terraform on each function.
- Staging is a queue and bronze is the record. The commit job never deletes a staged
  object — that would make its own retry unsafe — so the bucket lifecycle rule does.

## Then
Phase 2 (SPEC §12): five sources, Spark normalize into `silver.articles`, Glue-registered
Iceberg, and an Athena query with its bytes scanned and cost recorded.
