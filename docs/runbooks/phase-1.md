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
- [ ] Activate `project` as a cost-allocation tag — still not surfaced as of 2026-08-18
      20:15 UTC. The tag itself is fine: `get-tag-keys` returns `environment`,
      `managed_by`, `project`, and 21 resources carry `project=signal`, so Terraform's
      `default_tags` is working. `list-cost-allocation-tags` reads *billing data*, not the
      tagging API, and a tag appears there only after AWS processes a period in which a
      tagged resource actually cost something. Until this apply the only tagged resource
      was an empty S3 bucket. Re-check ~24h after **this apply**, not after 0.C's
      bootstrap:

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

## 1.D — The acceptance test, for real *(after apply)*
The test in `tests/test_replay_catchup.py` proves the mechanism against a temp warehouse.
Doing it against the deployed pipeline is the deliverable:

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
