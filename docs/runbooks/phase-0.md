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

## 0.C — AWS guardrails *(before any billable resource)*
- [ ] Root MFA; stop using root; admin identity via IAM Identity Center
- [ ] Budgets at $5 and $20; Cost Anomaly Detection; **confirm the alert email arrives** —
      an unconfirmed SNS subscription is a silent no-op
- [ ] Activate `project` as a cost-allocation tag (takes ~24 h to populate, which is why
      it belongs here and not in Phase 4)
- [ ] Verify the current free-tier egress allowance; write the real number into SPEC §10.1
- [ ] `terraform -chdir=infra/terraform/bootstrap apply -var state_bucket=<unique>`, then
      uncomment the backend block in `main/` and `terraform init -migrate-state`
- [ ] GitHub Actions OIDC role, read-only, for `terraform plan`. No long-lived keys, ever
- [ ] Confirm `terraform -chdir=infra/terraform/main plan` proposes **zero resources**

## 0.D-0.G *(done)*
- [x] Python scaffold, contracts, storage, fake source
- [x] Walking skeleton producing an HTML brief with a health footer
- [x] CI: lint, tests, skeleton-with-Spark, terraform validate, eval gates
- [x] Eval harness with 55 labeled pairs and enforced accuracy floors

## Then
Phase 1 (SPEC §12): three real pollers, `bronze.raw_documents` on Iceberg, and the replay
vs catch-up acceptance test.
