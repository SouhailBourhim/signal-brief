# Terraform state backend. Applied ONCE with local state, then `terraform init -migrate-state`
# moves this configuration's own state into the bucket it just created.
#
# Deliberately no DynamoDB lock table: S3 native locking (`use_lockfile`) has been the
# supported mechanism since Terraform 1.11, and a lock table is one more always-on resource
# to reason about under SPEC §10's cost discipline.

terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = "signal" # SPEC §10.2 — every resource, no exceptions
      managed_by  = "terraform"
      environment = "shared"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "state_bucket" {
  type        = string
  description = "Globally unique bucket name for Terraform state."
}

resource "aws_s3_bucket" "state" {
  bucket = var.state_bucket

  lifecycle {
    prevent_destroy = true # losing state is worse than losing the resources it tracks
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "state_bucket" {
  value = aws_s3_bucket.state.id
}
