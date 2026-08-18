# The pollers: one Lambda per source from one artifact, on an EventBridge schedule.
# SPEC §5, §6.1.
#
# `for_each` over var.sources is the mechanism behind SPEC §3's "adding source #6 must be
# a 30-minute job": the function, its schedule, its permissions, its log group, and its
# alarm all follow from one map entry.

# Built by `make lambda-package`, which installs Linux wheels for exactly the three
# libraries a poller imports (ADR-0006). Terraform will fail with "no such file" if that
# target hasn't run — deliberately: a plan that silently packaged an empty directory
# would deploy a Lambda that fails on import, at 3am, on a schedule.
data "archive_file" "poller" {
  type        = "zip"
  source_dir  = "${path.module}/../../../build/lambda"
  output_path = "${path.module}/../../../build/poll_source.zip"
}

resource "aws_iam_role" "poller" {
  name = "${var.name_prefix}-poller"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

data "aws_iam_policy_document" "poller" {
  # Write-only into staging, and only into staging. A poller that can read bronze is a
  # poller that can be made to egress it (SPEC §10.1), and it has no reason to read
  # anything it wrote.
  statement {
    sid       = "StageRawPayloads"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.bronze.arn}/${local.staging_prefix}/*"]
  }

  # One item, its own. GetItem/PutItem only: nothing here scans or deletes.
  statement {
    sid       = "ReadWriteOwnState"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem"]
    resources = [aws_dynamodb_table.state.arn]
  }

  # Constructed rather than referenced: the log groups are created by for_each in
  # monitoring.tf and referring to them here would need one statement per source for no
  # gain. CreateLogGroup is absent — Terraform makes them, so a function that somehow
  # logs somewhere unexpected fails loudly instead of provisioning itself a new group.
  statement {
    sid     = "WriteOwnLogs"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.name_prefix}-poll-*:*"
    ]
  }
}

resource "aws_iam_role_policy" "poller" {
  name   = "${var.name_prefix}-poller"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

resource "aws_lambda_function" "poller" {
  for_each = var.sources

  function_name = "${var.name_prefix}-poll-${each.key}"
  description   = each.value.description
  role          = aws_iam_role.poller.arn
  handler       = "poll_source.handler" # `make lambda-package` copies it in flat
  runtime       = "python3.12"
  architectures = ["x86_64"]

  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256

  timeout     = each.value.timeout_seconds
  memory_size = each.value.memory_mb

  # Ideally 1: a poller is not a service, and two overlapping invocations would race on
  # the same DynamoDB state item. It defaults to -1 (unreserved) because a new AWS
  # account's *total* concurrency limit is 10 and AWS refuses any reservation that takes
  # unreserved capacity below 10 — so reserving even one execution fails outright with
  # InvalidParameterValueException. Raise the account quota (Service Quotas
  # L-B99A9384), then set this to 1; see docs/runbooks/phase-1.md.
  #
  # What holds in the meantime: every cadence is far longer than its function's timeout
  # (120s against 5 minutes for hackernews, 60s against 15 for the others), and the
  # scheduler's retry window below is shorter than the gap to the next tick. If two ever
  # did overlap, the damage is a re-fetched window and a last-write-wins state item —
  # duplicates, which dedup collapses (SPEC §7.1), not loss.
  reserved_concurrent_executions = var.poller_reserved_concurrency

  environment {
    variables = {
      SOURCE_ID               = each.key
      STATE_TABLE_NAME        = aws_dynamodb_table.state.name
      BRONZE_STAGING_URI      = "s3://${aws_s3_bucket.bronze.id}/${local.staging_prefix}"
      SIGNAL_CONTACT_EMAIL    = var.contact_email
      PYTHONDONTWRITEBYTECODE = "1"
    }
  }

  depends_on = [aws_cloudwatch_log_group.poller]
}

# EventBridge Scheduler, not an EventBridge rule: it schedules without a bus, its free
# tier covers this volume many times over, and each schedule is independently pausable —
# which is exactly what SPEC §12's acceptance test does when it stops ingestion for a day.
resource "aws_iam_role" "scheduler" {
  name = "${var.name_prefix}-scheduler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "scheduler.amazonaws.com" }
      Condition = {
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.name_prefix}-scheduler-invoke"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = [for f in aws_lambda_function.poller : f.arn]
    }]
  })
}

resource "aws_scheduler_schedule" "poller" {
  for_each = var.sources

  name       = "${var.name_prefix}-poll-${each.key}"
  group_name = "default"

  # OFF: these cadences are already slower than each source moves, so spreading
  # invocations buys nothing and makes "when should this have run?" harder to answer
  # when the staleness check fires.
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = each.value.schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.poller[each.key].arn
    role_arn = aws_iam_role.scheduler.arn

    retry_policy {
      # A poller retries by being scheduled again — its next run re-reads the same
      # watermark and covers the same ground. The age limit is deliberately shorter than
      # the shortest cadence (5 minutes): with no reserved concurrency to throttle it, a
      # retry allowed to land late is the one way two invocations of the same source
      # could run at once.
      maximum_retry_attempts       = 1
      maximum_event_age_in_seconds = 120
    }
  }
}
