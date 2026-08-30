# Log retention, alarms, and where an alarm goes. SPEC §11, §10.3.
#
# The distinction that matters: CloudWatch here watches the *infrastructure* — did the
# function run, did it crash, was it throttled. It cannot see the thing most likely to go
# wrong, which is a feed returning 200 with nothing new in it for six hours (SPEC §3).
# That is `ops.source_health` in the Airflow DAG, computed from fetch metadata in bronze.
# Alarms below are the backstop, not the monitoring.

resource "aws_cloudwatch_log_group" "poller" {
  for_each = var.sources

  name = "/aws/lambda/${var.name_prefix}-poll-${each.key}"
  # Default retention is "never expire", which is how a $0 logging bill becomes a real
  # one. Two weeks outlives any recovery window Airflow replays.
  retention_in_days = 14
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.contact_email

  # AWS emails a confirmation link; until it is clicked the subscription sits in
  # "PendingConfirmation" and alarms go nowhere. Terraform cannot confirm it, and
  # cannot tell the difference either — see the Phase 1 runbook's verification step.
  # SPEC §0.C made the same point about budget alerts: an unconfirmed alert is no alert.
}

# The three alarms below watch the poller fleet in aggregate, not one set per source. That
# is a deliberate trade against the free tier's ceiling of 10 alarms: three per source
# across nine sources, plus the two local alarms further down, is 29 alarms, and every one
# past the tenth bills $0.10/month for coverage that `ops.source_health` already provides
# with more detail. The AWS/Lambda namespace publishes each of these metrics at the account
# level with no dimensions, so one alarm watches the whole fleet for one alarm's worth of
# cost.
#
# What that gives up is attribution: a fired alarm says "a poller", not which one. The log
# groups above and `ops.source_health` say which. What it does not give up is detection,
# because all three conditions are fleet-scoped in practice — a structural error, an
# account-wide concurrency limit, and a deleted schedule group reach every poller or none.
#
# This holds because lambda.tf's `poller` for_each is the only aws_lambda_function in
# main/, which makes "account-wide" and "the poller fleet" the same set. Adding a
# non-poller Lambda would silently widen all three, and is the point at which they need a
# Metrics Insights query scoped to `signal-poll-%` rather than a bare namespace.

resource "aws_cloudwatch_metric_alarm" "poller_errors" {
  alarm_name        = "${var.name_prefix}-pollers-errors"
  alarm_description = <<-EOT
    A poller raised. Every expected failure — a 500, a timeout, an unreachable host — is
    already handled inside the function as an outcome=ERROR document (SPEC §6.2), so
    reaching here means something structural: DynamoDB, S3, IAM, or a bug. Treat it as a
    real page, not a flapping feed.

    This alarm does not name the source. `/aws/lambda/${var.name_prefix}-poll-*` does —
    the failing invocation is in whichever log group has an ERROR in the last 15 minutes.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Errors"

  statistic           = "Sum"
  period              = 900
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching" # no invocations in the window is not an error

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "poller_silent" {
  alarm_name        = "${var.name_prefix}-pollers-not-running"
  alarm_description = <<-EOT
    No poller has been invoked for an hour. This is the failure the error alarm cannot
    see: a deleted schedule group, a disabled schedule, or an IAM change means zero
    invocations and therefore zero errors. Silence looks identical to health on every
    other metric.

    Threshold 1 means "nothing ran at all" — the only cadence-independent statement
    available fleet-wide. The floor is normally 36 invocations an hour, but tightening
    toward it would make this a thing to re-tune on every change to var.sources. A single
    dead source with the rest healthy is `ops.source_health`'s job (ops/monitor.py::assess),
    which sees per-source staleness an invocation count cannot.

    Per-source alarms were also wrong for the daily sources: a fixed one-hour period
    against `market` (02:11) and `macro` (02:26) left both in ALARM 23 hours a day.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Invocations"

  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching" # no datapoint *is* the condition being watched

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "poller_throttled" {
  alarm_name        = "${var.name_prefix}-pollers-throttled"
  alarm_description = <<-EOT
    A poller was throttled, so a scheduled poll did not run. With no per-function
    reservation (see lambda.tf) this is the account-wide concurrency limit — 10 on a new
    account — being hit, most likely because a poll is now outlasting its schedule
    interval. For hackernews that would mean the item backlog is growing faster than
    MAX_ITEMS_PER_POLL drains it.

    Concurrency is an account-level resource, so this alarm was already measuring an
    account-level condition when it was per-source; nine copies of it reported the same
    limit nine times.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Throttles"

  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
}

output "bronze_bucket" {
  value = aws_s3_bucket.bronze.id
}

output "staging_uri" {
  description = "SIGNAL_ / BRONZE_STAGING_URI for local runs of the commit job."
  value       = "s3://${aws_s3_bucket.bronze.id}/${local.staging_prefix}"
}

output "warehouse_uri" {
  description = "SIGNAL_ICEBERG_WAREHOUSE — set this and the commit job writes to Glue + S3."
  value       = "s3://${aws_s3_bucket.bronze.id}/${local.warehouse_prefix}"
}

output "state_table_name" {
  value = aws_dynamodb_table.state.name
}

output "poller_function_names" {
  value = [for f in aws_lambda_function.poller : f.function_name]
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

# ── The local half ────────────────────────────────────────────────────────────────────
#
# SPEC §11's monitoring covers the AWS side. `docs/runbooks/phase-4b.md` records the gap that
# leaves: ten hours of dead ingestion produced no signal behind a green console, because every
# alarm above watches a Lambda and the Lambdas were fine — it was the laptop that had stopped.
#
# The two alarms below watch the two ways the local half fails, and they are not
# interchangeable. A task failing with the scheduler alive is reported by an Airflow callback
# (`airflow/dags/alerting.py`). A scheduler frozen with a suspended host cannot report anything
# at all — on 2026-08-24 the containers read `Up` throughout — so that case needs something
# outside the laptop watching for silence. Metrics come from `signal_core/ops/heartbeat.py`.

resource "aws_cloudwatch_metric_alarm" "local_silent" {
  alarm_name        = "${var.name_prefix}-local-not-running"
  alarm_description = <<-EOT
    No Airflow DAG has completed on the local host for three hours. ingest_monitor runs
    hourly, so three missed beats means the scheduler is not running — most likely the host
    is suspended (ADR-0002 puts everything interpretive on a laptop, and a laptop sleeps).

    This is the failure an on_failure_callback structurally cannot report: the process that
    would send it is the process that is gone. Nothing in AWS looks wrong while this is true;
    the pollers keep filling staging and nothing local is left to commit or read it.
  EOT

  namespace   = "Signal/Local"
  metric_name = "LocalHeartbeat"

  statistic           = "Sum"
  period              = 10800 # three ingest_monitor beats
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching" # no datapoint *is* the condition being watched

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "local_task_failed" {
  alarm_name        = "${var.name_prefix}-local-task-failed"
  alarm_description = <<-EOT
    An Airflow task failed on the local host. The scheduler was alive to notice, which is
    what distinguishes this from the silence alarm above; the DAG and task are on the
    Signal/Local DagFailure metric's dimensions, and in the task log.
  EOT

  namespace   = "Signal/Local"
  metric_name = "LocalFailure"

  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  # Unlike the silence alarm, absence here is health: most hours have no failures.
  treat_missing_data = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
}
