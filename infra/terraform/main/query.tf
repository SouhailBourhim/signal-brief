# Glue databases for silver/ops, Athena's results bucket and workgroup, and the
# `signal-analyst` role ad-hoc queries run as. SPEC §5, §9, §10.3, §17.
#
# `silver` and `ops` were already being created by Iceberg's `CREATE NAMESPACE IF NOT
# EXISTS`, running as the admin identity every job connects with — it works, but a
# namespace conjured that way is untagged, invisible in `terraform plan`, and outside
# `terraform destroy`'s reach if this project is ever torn down. Declaring them here
# doesn't change where a table lives (Iceberg's `CREATE TABLE IF NOT EXISTS` targets the
# same Glue database either way); it makes the database itself a tracked resource.

resource "aws_glue_catalog_database" "silver" {
  name        = "silver"
  description = "Parsed, deduplicated articles and comments. SPEC §7, §9."
}

resource "aws_glue_catalog_database" "ops" {
  name        = "ops"
  description = "Pipeline health and cost, as data rather than a dashboard. SPEC §9, §11."
}

# Created until now by `ops/athena.py::create_iceberg_table`'s `CREATE SCHEMA IF NOT
# EXISTS`, running as the admin identity every Athena write connects with — the exact
# situation the comment above describes for silver and ops, one namespace later. It also
# had a second consequence the others didn't: the analyst policy below enumerates the
# databases it grants, so a namespace Terraform didn't know about was one `signal-analyst`
# could not read. The gold marts are the tables an outside reader most wants.
resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Serving marts: brief items, cluster enrichment, macro observations. SPEC §9."
}

# Query scratch, not the record: no prevent_destroy, unlike bronze.
resource "aws_s3_bucket" "athena_results" {
  bucket = var.athena_results_bucket
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  # Every result set is reproducible by re-running the query, so nothing here needs to
  # outlive the debugging session it was written for.
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}

# `enforce_workgroup_configuration = true` makes this workgroup's settings win over
# whatever a client requests, including the one guardrail that actually matters:
# `bytes_scanned_cutoff_per_query`. Without it, a `SELECT *` against a table of raw
# payloads is a query that ignores every other cost control in this project.
resource "aws_athena_workgroup" "signal" {
  name = "signal"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # 100 MB, not the 10 MB API minimum: `ops.athena.athena_cost_usd` already floors the
    # *reported* cost at Athena's real per-query minimum, so this cutoff exists purely to
    # catch a runaway query before it scans the whole table, not to police tiny ones.
    bytes_scanned_cutoff_per_query = 104857600

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Assumable by the admin IAM user (ADR-0005) rather than always querying with the admin
# key directly. `sts:AssumeRole` still requires a deliberate step, so it doesn't happen
# by accident, but it means "query the lake" and "administer the account" are different
# credentials in practice, which is the property SPEC §17 asks for.
resource "aws_iam_role" "analyst" {
  name = "${var.name_prefix}-analyst"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/${var.admin_iam_user_name}" }
    }]
  })
}

data "aws_iam_policy_document" "analyst" {
  statement {
    sid = "RunQueriesOnTheSignalWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.signal.arn]
  }

  statement {
    sid = "ReadTheCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/bronze",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.ops.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/bronze/*",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.ops.name}/*",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
    ]
  }

  # A BI client browses before it queries: Power BI's navigator lists catalogs, then
  # databases, then tables, through Athena's metadata API rather than Glue's (docs/
  # powerbi.md). `signal athena-query` never calls these because it is handed the database
  # name, which is why the gap only appears the first time something connects with a driver.
  statement {
    sid = "BrowseTheCatalogFromABIClient"
    actions = [
      "athena:GetDataCatalog",
      "athena:ListDatabases",
      "athena:GetDatabase",
      "athena:ListTableMetadata",
      "athena:GetTableMetadata",
    ]
    resources = [
      "arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:datacatalog/AwsDataCatalog",
    ]
  }

  # `athena:ListDataCatalogs` has no resource type — it is the call that discovers which
  # catalog ARNs exist, so it cannot be scoped to one. Written as `*` deliberately rather
  # than by omission: a datacatalog ARN here would match nothing and fail at connect time
  # with an AccessDenied naming no resource, which is ADR-0005's failure mode again. It
  # returns catalog names and nothing else; the statement above is what gates reading them.
  statement {
    sid       = "DiscoverWhichCatalogsExist"
    actions   = ["athena:ListDataCatalogs"]
    resources = ["*"]
  }

  # Athena executes as the calling principal, not a service role — reading the actual
  # table data means reading bronze's S3 objects directly, same as the local Spark jobs.
  statement {
    sid       = "ReadBronzeData"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.bronze.arn}/${local.warehouse_prefix}/*"]
  }

  statement {
    sid       = "ListBronzeAndResults"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.bronze.arn, aws_s3_bucket.athena_results.arn]
  }

  # Athena writes every result set here and reads it back to serve GetQueryResults.
  statement {
    sid       = "ReadWriteQueryResults"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.athena_results.arn}/*"]
  }
}

resource "aws_iam_role_policy" "analyst" {
  name   = "${var.name_prefix}-analyst"
  role   = aws_iam_role.analyst.id
  policy = data.aws_iam_policy_document.analyst.json
}

output "analyst_role_arn" {
  description = "aws sts assume-role --role-arn <this> --role-session-name athena-query"
  value       = aws_iam_role.analyst.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.signal.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.id
}
