# Backend, provider, and the variables every other file in this directory reads.
#
# Phase 1's resources live in storage.tf (S3 / DynamoDB / Glue), lambda.tf (the pollers
# and their schedules), and monitoring.tf (log groups, alarms, alerts). Every one of them
# is either always-free or free at this volume — SPEC §10.

terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }

  backend "s3" {
    bucket       = "signal-brief-tfstate-481879233905"
    key          = "main/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = "signal"
      managed_by  = "terraform"
      environment = var.environment
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "name_prefix" {
  description = "Prefixes every resource name, so two environments can share an account."
  type        = string
  default     = "signal"
}

variable "bronze_bucket" {
  description = "Globally unique bucket for bronze and the staging prefix. SPEC §6.4."
  type        = string
  default     = "signal-bronze-481879233905"
}

variable "contact_email" {
  description = <<-EOT
    Goes into the pollers' User-Agent and receives alarm notifications. SEC EDGAR
    requires a contact address in the User-Agent and blocks fair-access violators, so
    this is a functional value, not documentation. SPEC §6.2.
  EOT
  type        = string
  default     = "bourhimsouhail@gmail.com"
}

variable "poller_reserved_concurrency" {
  description = <<-EOT
    Reserved concurrent executions per poller. -1 means unreserved, which is the only
    value a new AWS account accepts: the default account limit is 10 concurrent
    executions and AWS will not let a reservation take unreserved capacity below 10.
    Set to 1 once the account quota is raised — see lambda.tf for what holds until then.
  EOT
  type        = number
  default     = -1
}

variable "sources" {
  description = <<-EOT
    Poller definitions. One map entry -> one Lambda, one schedule, one alarm, one log
    group. Adding source #6 is this map plus a module in signal_core/sources — the
    30-minute claim SPEC §3 makes rests on nothing else being involved.

    `schedule_expression` is EventBridge Scheduler syntax. Cadences come from SPEC §3 and
    are deliberately slower than the source allows: the free tier is generous, but SEC
    fair-access limits and RSS etiquette are not, and nothing downstream reads faster
    than the daily brief.
  EOT
  type = map(object({
    schedule_expression = string
    timeout_seconds     = optional(number, 60)
    memory_mb           = optional(number, 256)
    description         = optional(string, "")
  }))
  default = {
    hackernews = {
      schedule_expression = "rate(5 minutes)"
      timeout_seconds     = 120 # walks up to 200 sequential item ids per invocation
      description         = "Hacker News items by id. Backfill horizon: complete."
    }
    edgar = {
      schedule_expression = "rate(15 minutes)"
      description         = "SEC EDGAR current filings atom feed. Backfill horizon: ~1 day."
    }
    rss_tech = {
      schedule_expression = "rate(15 minutes)"
      description         = "TechCrunch RSS. Backfill horizon: the feed window only."
    }
  }
}

locals {
  # The staging prefix the pollers write to and `spark/jobs/commit_bronze.py` reads.
  # Bronze itself is written by Spark on commit, under bronze/.
  staging_prefix = "staging"
  bronze_prefix  = "bronze"
}

data "aws_caller_identity" "current" {}
