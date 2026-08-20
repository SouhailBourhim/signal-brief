# Phase 0 runbook

Exit condition: a fresh clone runs `make setup && make skeleton && make test && make eval`
green, CI is green, and **zero AWS resources exist beyond guardrails and Terraform state**.

## 0.A — Repo *(done)*
- [x] Git repo, `.gitignore`, MIT license, README
- [x] ADR-0001 (no Kafka), ADR-0002 (runtime shape), ADR-0004 (package name)
- [x] `gh repo create signal-brief --public --source . --push` —
      https://github.com/SouhailBourhim/signal-brief

## 0.B — Dev machine *(Ryzen box)* *(done)*
- [x] WSL2 + Ubuntu 24.04 if on Windows; clone **inside** the WSL2 filesystem, never `/mnt/c`
- [x] JDK 17 (Temurin) — Spark 4 requires 17+
- [x] `uv`, then `uv python install 3.12`
- [x] Docker, Terraform ≥ 1.11, AWS CLI v2, `gh`
- [x] Ollama on the **host**; pull an 8B-class q4 model
- [x] Record digest + tokens/sec in ADR-0003, then rewrite SPEC §7.3's capacity paragraph
- [x] `make setup && make skeleton` — first run with a real JVM

## 0.C — AWS guardrails *(before any billable resource)* *(done)*
- [x] Root MFA (already enabled); admin identity via a plain IAM user, not Identity Center —
      ADR-0005 records why (Identity Center requires an Organization, which irreversibly
      upgrades a Free Plan account and forfeits its credit balance)
- [x] Budgets at $5 and $20; Cost Anomaly Detection; **confirmed the alert email arrives** —
      already confirmed for bourhimsouhail@gmail.com via AWS's auto-created default
      subscription; tightened its threshold from $100/40% to $1
- [x] Activate `project` as a cost-allocation tag — **done 2026-08-20**. Pending for two
      days, and not for anything in this repo: `list-cost-allocation-tags` reads *billing*
      data, so a tag only becomes activatable after AWS processes a period in which a
      tagged resource actually cost money. Once the ingest apply gave it one, it appeared
      as `Inactive` and one `update-cost-allocation-tags-status` call flipped it to
      `Active`. §10.3's "what did ingestion cost?" is answerable per project from here on
- [x] Verify the current free-tier egress allowance; write the real number into SPEC §10.1 —
      100 GB/month confirmed against AWS's own pricing page, unchanged
- [x] `terraform -chdir=infra/terraform/bootstrap apply -var state_bucket=<unique>`, then
      uncomment the backend block in `main/` and `terraform init -migrate-state` —
      signal-brief-tfstate-481879233905
- [x] GitHub Actions OIDC role, read-only, for `terraform plan`. No long-lived keys, ever —
      trust policy had to match GitHub's immutable subject-claim format (ADR-0005); role is
      ReadOnlyAccess and CI's plan runs with `-lock=false` since that role can't write the
      S3 lock object
- [x] Confirm `terraform -chdir=infra/terraform/main plan` proposes **zero resources** —
      confirmed locally and in CI

## 0.D-0.G *(done)*
- [x] Python scaffold, contracts, storage, fake source
- [x] Walking skeleton producing an HTML brief with a health footer
- [x] CI: lint, tests, skeleton-with-Spark, terraform validate, eval gates
- [x] Eval harness with 55 labeled pairs and enforced accuracy floors

## Then
Phase 1 (SPEC §12): three real pollers, `bronze.raw_documents` on Iceberg, and the replay
vs catch-up acceptance test.
