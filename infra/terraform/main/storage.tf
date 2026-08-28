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

# Makes "bronze is immutable" structural rather than conventional.
#
# The claim is already enforced where it can be: the poller role is PutObject-only on
# `staging/*` (lambda.tf) and the Athena role is read-only (query.tf). But the Spark commit
# job runs locally under a developer's own credentials, and on that side nothing but care
# stands between a mistyped `overwritePartitions` and the one dataset in this project that
# cannot be recomputed — no source lets us re-fetch yesterday's bytes (SPEC §6.2, §6.3).
# `prevent_destroy` guards the bucket; it says nothing about the objects inside it.
#
# Versioning turns an overwrite or a delete into a recoverable event rather than a
# permanent one. It is also the mitigation for a secret written into an object by accident:
# the fix for that is deleting the object, and without versioning "delete" has to mean the
# whole object with no way back if the deletion was itself the mistake.
#
# Object Lock would be stronger, and is deliberately not used: it can only be enabled at
# bucket creation, and this bucket exists and holds real data. Versioning is the strongest
# option that can be applied in place.
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  # Ordering matters here only in that versioning must exist before rules that talk about
  # noncurrent versions; Terraform infers it from this reference.
  depends_on = [aws_s3_bucket_versioning.bronze]

  # Staging is a queue, not a store (ADR-0006). Objects are safe to expire once the
  # commit job has merged them into bronze.raw_documents; 14 days is far longer than
  # any recovery window Airflow's DAG will replay, and short enough that a forgotten
  # backlog cannot quietly become the bill.
  #
  # `noncurrent_version_expiration` is not optional now the bucket is versioned: with
  # versioning on, `expiration` stops deleting anything and starts writing a delete marker
  # over a retained version, so without this the 14-day rule would have quietly become a
  # 14-day rule that never reclaims a byte. One day, because a staged object has no value
  # at all once committed — the recovery story for staging is re-polling, not undelete.
  rule {
    id     = "expire-staged-objects"
    status = "Enabled"
    filter {
      prefix = "${local.staging_prefix}/"
    }
    expiration {
      days = 14
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  # The recovery window the versioning above exists to provide, and its cost bound.
  #
  # Iceberg rewrites data files on compaction and unlinks them at `expire_snapshots`; under
  # versioning every one of those becomes a retained noncurrent version that is billed and
  # invisible in a normal listing. 30 days is chosen as the shortest window that still
  # covers the realistic case — an overwrite noticed when a brief looks wrong, which is
  # days later, not minutes — while keeping the retained set to roughly a month of
  # compaction churn rather than an unbounded second copy of the lake.
  rule {
    id     = "expire-noncurrent-warehouse-objects"
    status = "Enabled"
    filter {
      prefix = "${local.warehouse_prefix}/"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Once every version behind a delete marker has expired, the marker itself is left
  # behind. Harmless individually, but this bucket deletes many small objects and they
  # accumulate into slower listings for no benefit.
  rule {
    id     = "clean-expired-delete-markers"
    status = "Enabled"
    filter {}
    expiration {
      expired_object_delete_marker = true
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

  # Current versions of warehouse objects are never expired on a timer: Iceberg decides
  # when a data file stops being referenced, and a lifecycle rule that second-guessed it
  # would delete live table data.
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
