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

variable "athena_results_bucket" {
  description = "Globally unique bucket for Athena query results. Separate from bronze (SPEC §10.3): bronze is prevent_destroy and the immutable record; query scratch is neither."
  type        = string
  default     = "signal-athena-results-481879233905"
}

variable "admin_iam_user_name" {
  description = <<-EOT
    The plain IAM user Terraform and local CLI work run as (ADR-0005). `signal-analyst`
    trusts this identity to assume it, so ad-hoc queries run under a scoped-down role
    instead of the admin key directly — SPEC §17: "I query with an admin key" undoes
    least-privilege even when the identity behind it is trustworthy.
  EOT
  type        = string
  default     = "Souhail_Signal_Admin"
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

variable "poller_schedule_state" {
  description = <<-EOT
    ENABLED or DISABLED for every poller schedule at once. Exists for SPEC §12's Phase 1
    acceptance test (1.D), which requires stopping ingestion for a day and restarting it.

    Declarative rather than `aws scheduler update-schedule` by hand, for two reasons.
    That CLI call is a full replace — it re-sends the target and flexible-time-window or
    silently drops them, so hand-disabling six schedules risks losing the retry policy on
    the way back. And with the state unmanaged, Terraform assumes the ENABLED default:
    any `terraform apply` during the outage — including one for an unrelated change —
    would quietly switch ingestion back on and void a test that takes a day to run.
  EOT
  type        = string
  default     = "ENABLED"

  validation {
    condition     = contains(["ENABLED", "DISABLED"], var.poller_schedule_state)
    error_message = "poller_schedule_state must be ENABLED or DISABLED."
  }
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
    # Phase 2. Six pollers now run unreserved against a new account's total concurrency
    # limit of 10, and all six collide at :00/:15/:30/:45. It still fits — source #7 is
    # where it stops fitting, and where Service Quota L-B99A9384 stops being optional.
    edgar_formd = {
      schedule_expression = "rate(15 minutes)"
      description         = "SEC Form D current filings. Backfill horizon: ~1 day, not complete."
    }
    rss_verge = {
      schedule_expression = "rate(15 minutes)"
      description         = "The Verge Atom. Backfill horizon: the feed window only."
    }
    rss_ars = {
      schedule_expression = "rate(15 minutes)"
      description         = "Ars Technica RSS 2.0. Backfill horizon: the feed window only."
    }
    # Phase 4A, and the source that reaches #7 — the point the comment above named as where
    # six colliding pollers stop fitting under a new account's concurrency limit of 10.
    #
    # It fits because it does not collide. `rate(...)` gives no control over phase, so this
    # is the first schedule in the map expressed as cron: :07/:22/:37/:52 misses the
    # :00/:15/:30/:45 pileup the other five share, and misses every multiple of 5, which is
    # where `hackernews` lands. Peak concurrency stays where it was instead of going to 7,
    # and Service Quota L-B99A9384 stays optional.
    hn_scores = {
      schedule_expression = "cron(7,22,37,52 * * * ? *)"
      timeout_seconds     = 120 # TOP_N=60 items per invocation, paced at 5/sec
      description         = "HN top-story score snapshots for §7.4 velocity. Horizon: window."
    }
    # Source #8, once a day. 02:11 UTC is after the US close (20:00 UTC + settlement) and
    # before the 04:30/05:00 processing chain, so the brief reads bars that are already
    # committed. Off the :00 minute for the same reason `hn_scores` is: nothing else runs
    # at :11, so this never adds to a concurrency peak.
    market = {
      schedule_expression = "cron(11 2 * * ? *)"
      timeout_seconds     = 120 # one request per watchlist ticker, paced at 2/sec
      description         = "Daily OHLCV for watchlist tickers, §7.4 market corroboration."
    }
    # Phase 4B — SPEC §8's bitemporal macro store. 02:26 UTC, clear of `market` at 02:11 and
    # of every `rate(15 minutes)` tick (:00/:15/:30/:45), so peak concurrency is unchanged
    # and Service Quota L-B99A9384 stays optional — the property
    # `test_the_phase_4a_pollers_do_not_collide_with_the_phase_1_2_six` asserts.
    #
    # Daily is generous for data that releases monthly, and deliberately so: a release lands
    # on a weekday morning US time and the revision it carries is what SPEC §8's brief line
    # is about, so waiting a month to notice would make the feature pointless. Six requests a
    # day against a free API is not a volume anyone will mind.
    macro = {
      schedule_expression = "cron(26 2 * * ? *)"
      # Six sequential requests, each returning ~a decade of vintages. Well under the
      # Lambda's ceiling, generously over what the requests actually take.
      timeout_seconds = 180
      memory_mb       = 512 # all-vintages responses are megabytes, not kilobytes
      description     = "ALFRED macro vintages for the watchlist series, §8 bitemporal store."
    }
  }
}

locals {
  # The staging prefix the pollers write to and `spark/jobs/commit_bronze.py` reads.
  staging_prefix = "staging"
  # Historically "bronze_prefix": the Iceberg warehouse *root*, not just the bronze
  # table's location. It already holds `ops.db` (health_snapshot.py, cost_snapshot.py)
  # and now `silver.db` (2.C) alongside `bronze.db` — value-preserving rename, since
  # existing table locations are recorded in Glue and don't move (docs/runbooks/
  # phase-2.md 2.D).
  warehouse_prefix = "bronze"
}

data "aws_caller_identity" "current" {}
