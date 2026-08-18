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

## 1.B — Infrastructure *(written and planned; not applied)*
- [x] `infra/terraform/main` — S3 bronze bucket (SSE, public access blocked, staging
      expiry at 14 days), DynamoDB state table, Glue database `bronze`, three Lambdas via
      `for_each` over `var.sources`, EventBridge Scheduler schedules, log groups with
      14-day retention, SNS alerts, and three alarms per source
- [x] `make lambda-package` — 11 MB artifact: httpx, pydantic, pydantic-settings, and the
      source tree. `tests/test_lambda_artifact.py` fails if the handler's import chain
      ever acquires pyarrow, pyspark, pandas, numpy, or jinja2
- [x] `terraform plan` — **30 to add, 0 to change, 0 to destroy**, run 2026-08-18 against
      account 481879233905. Nothing applied
- [ ] `terraform apply`. Before running it, know that this is where the account starts
      spending. Everything provisioned is inside the always-free tier at these cadences
      (Lambda 1M requests/month, DynamoDB 25 GB, EventBridge Scheduler 14M invocations,
      Glue's first 1M objects); S3 storage and requests are the only real line item, and
      the $5/$20 budgets from 0.C are already watching them
- [ ] **Confirm the SNS subscription email.** Terraform creates it in
      `PendingConfirmation` and cannot tell the difference — an unconfirmed subscription
      means every alarm below fires into nothing. Same trap as 0.C's budget alerts
- [ ] Re-check the cost-allocation tag from 0.C — `aws ce list-cost-allocation-tags`

## 1.C — Orchestration *(written; not yet run against real data)*
- [x] `airflow/dags/ingest_monitor_dag.py` — hourly: commit the staged interval, assess
      each source over the hour that just closed, write `ops.source_health`, fail the run
      when a source is stale or gapped
- [ ] `make up`, unpause `ingest_monitor`, and watch one real hour through it

## 1.D — The acceptance test, for real *(after apply)*
The test in `tests/test_replay_catchup.py` proves the mechanism against a temp warehouse.
Doing it against the deployed pipeline is the deliverable:

1. Disable the three EventBridge schedules. Note the time.
2. Leave it off for a day. The brief's footer should degrade as each source passes its
   freshness SLA — 1 minute for `hackernews`, 15 for `edgar`, 30 for `rss_tech`.
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
