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

resource "aws_cloudwatch_metric_alarm" "poller_errors" {
  for_each = var.sources

  alarm_name        = "${var.name_prefix}-poll-${each.key}-errors"
  alarm_description = <<-EOT
    ${each.key}: the poller raised. Every expected failure — a 500, a timeout, an
    unreachable host — is already handled inside the function as an outcome=ERROR
    document (SPEC §6.2), so reaching here means something structural: DynamoDB, S3,
    IAM, or a bug. Treat it as a real page, not a flapping feed.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Errors"
  dimensions  = { FunctionName = aws_lambda_function.poller[each.key].function_name }

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
  for_each = var.sources

  alarm_name        = "${var.name_prefix}-poll-${each.key}-not-running"
  alarm_description = <<-EOT
    ${each.key} has not been invoked for an hour. This is the failure the error alarm
    cannot see: a disabled schedule, a deleted target, or an IAM change means zero
    invocations and therefore zero errors. Silence looks identical to health on every
    other metric.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Invocations"
  dimensions  = { FunctionName = aws_lambda_function.poller[each.key].function_name }

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
  for_each = var.sources

  alarm_name        = "${var.name_prefix}-poll-${each.key}-throttled"
  alarm_description = <<-EOT
    ${each.key} was throttled. With reserved concurrency of 1 this means invocations are
    overlapping — the poll is taking longer than its schedule interval, which for
    hackernews means the item backlog is growing faster than MAX_ITEMS_PER_POLL drains it.
  EOT

  namespace   = "AWS/Lambda"
  metric_name = "Throttles"
  dimensions  = { FunctionName = aws_lambda_function.poller[each.key].function_name }

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
  value       = "s3://${aws_s3_bucket.bronze.id}/${local.bronze_prefix}"
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
