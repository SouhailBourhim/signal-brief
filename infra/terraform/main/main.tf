# Everything that is not the state backend. Phase 0 provisions guardrails only:
# nothing here is billable, and `terraform plan` proposing zero resources is the Phase 0
# exit condition.
#
# Phase 1 adds S3 bronze, DynamoDB state, Glue catalog, the Lambda pollers, and their
# EventBridge schedules — via `for_each` over `var.sources`, so a new source is one map
# entry (see handlers/poll_source.py).

terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }

  # Filled in after the bootstrap module has been applied:
  # backend "s3" {
  #   bucket       = "<state_bucket output>"
  #   key          = "main/terraform.tfstate"
  #   region       = "us-east-1"
  #   encrypt      = true
  #   use_lockfile = true
  # }
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

variable "sources" {
  description = "Poller definitions. One map entry per source -> one Lambda + schedule."
  type = map(object({
    schedule_expression = string
    timeout_seconds     = optional(number, 30)
    memory_mb           = optional(number, 256)
  }))
  default = {}
}
