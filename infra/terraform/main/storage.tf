# S3, DynamoDB, and the Glue database Iceberg registers tables in. SPEC §5, §6.4, §10.

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket

  lifecycle {
    # Bronze is the one thing in this project that cannot be recomputed: everything
    # downstream is derived from these bytes, and no source lets us re-fetch them
    # (SPEC §6.2, §6.3). A `terraform destroy` must not be able to take it.
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      # SSE-S3, not KMS: KMS bills per request, and this bucket's access pattern is
      # many small objects (SPEC §10.3).
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  # Staging is a queue, not a store (ADR-0006). Objects are safe to expire once the
  # commit job has merged them into bronze.raw_documents; 14 days is far longer than
  # any recovery window Airflow's DAG will replay, and short enough that a forgotten
  # backlog cannot quietly become the bill.
  rule {
    id     = "expire-staged-objects"
    status = "Enabled"
    filter {
      prefix = "${local.staging_prefix}/"
    }
    expiration {
      days = 14
    }
  }

  # A failed multipart upload leaves parts that are billed and invisible in the console.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Iceberg rewrites data files on compaction; the old ones become unreferenced only
  # after expire_snapshots runs, so nothing here deletes them on a timer.
}

resource "aws_dynamodb_table" "state" {
  name         = "${var.name_prefix}-pipeline-state"
  billing_mode = "PAY_PER_REQUEST" # 25 GB and on-demand requests are free at this volume
  hash_key     = "source_id"

  attribute {
    name = "source_id"
    type = "S"
  }

  # One small item per source, read and written every poll (signal_core/state_store.py).
  # PITR is off: the item is a watermark, not a record — losing it costs one duplicated
  # poll, which dedup collapses (SPEC §7.1), and PITR is billed per GB-month.
  point_in_time_recovery {
    enabled = false
  }

  lifecycle {
    prevent_destroy = true # losing watermarks means a gap or a re-fetch storm
  }
}

# Iceberg's GlueCatalog maps a namespace to a Glue database, so `bronze.raw_documents`
# needs a database called `bronze`. No crawler and no Glue ETL job is involved — SPEC §5
# excludes both; Spark writes the table and Iceberg registers it.
resource "aws_glue_catalog_database" "bronze" {
  name        = "bronze"
  description = "Immutable raw documents as fetched. SPEC §6.4."
}
