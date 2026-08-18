# ADR-0005 — AWS admin identity, budgets, and GitHub OIDC (Phase 0 0.C)

**Status:** Accepted · **Date:** 2026-08-18

## Context

SPEC's non-negotiable rules require least-privilege IAM and no long-lived keys in CI. The
Phase 0 runbook named IAM Identity Center as the mechanism for "an admin identity that
isn't root." Setting up Identity Center requires an AWS Organization, and — discovered
while actually doing this, not assumed — creating an Organization on an AWS Free Plan
account is an irreversible, immediate upgrade to pay-as-you-go pricing that forfeits any
free-tier credit balance on the account. This account had $100 (rising to $200) of
signup credit riding on staying on the Free Plan.

## Decision

**Admin identity:** a plain IAM user (`Souhail_Signal_Admin`) with `AdministratorAccess`
attached directly, MFA required, used only for local CLI/Terraform work via access keys
stored in the local (gitignored, never-committed) AWS credentials file. This satisfies the
actual property the runbook wanted — routine work never runs as root — without requiring
an Organization. The Free Plan's documented restrictions (Reserved Instances, Savings
Plans, Marketplace, Support plans, joining an Organization) don't touch anything this
project needs: S3, Lambda, EventBridge, Glue, Athena, Budgets, IAM users all work
unrestricted on the Free Plan.

**GitHub Actions role:** `signal-brief-gha-terraform-plan`, assumed via OIDC
(`token.actions.githubusercontent.com`), trust policy scoped to
`repo:SouhailBourhim/signal-brief:*` — no other repo can assume it, and no AWS access
keys exist in GitHub secrets. Attached policy is the AWS-managed `ReadOnlyAccess` policy
rather than a hand-rolled least-privilege list: this project's resource surface (S3,
DynamoDB, Glue, Lambda, EventBridge, Athena) grows through Phase 1–3, and a hand-maintained
allow-list would either lag behind and break CI's `terraform plan`, or get maintained by
periodically widening it anyway. `ReadOnlyAccess` is broad but categorically cannot
mutate anything — the property that actually matters for a plan-only role. Wired into
`ci.yml`'s `terraform` job so the role is exercised on every push/PR, not left idle.

**Budgets and Cost Anomaly Detection** were created directly via the AWS CLI rather than
Terraform, to keep `infra/terraform/main`'s resource count at zero per Phase 0's exit
condition. AWS now auto-provisions a default Cost Anomaly Detection monitor + subscription
on new accounts; the email (`bourhimsouhail@gmail.com`) was already confirmed, so nothing
was pending there — its threshold ($100 / 40% impact) was tightened to $1 absolute, since
$100 would never fire at this project's scale. A monthly `$20` account-wide budget was
created with email alerts at $5 (25%) and $20 (100% actual), plus a forecasted-100% alert.

## Consequences

- `signal_core`/Terraform never see a long-lived AWS credential. Local dev uses an IAM
  user's keys (acceptable for a single-developer account); CI uses OIDC exclusively.
- The `ReadOnlyAccess` policy on the GitHub role is intentionally broader than strict
  least-privilege — revisit if this repo ever gains collaborators or the account gains
  other tenants, neither of which is true today.
- Cost-allocation tag activation for `project=signal` is still pending: AWS only offers a
  tag for activation once a tagged resource has appeared in billing data, which lags by
  up to 24h after the bootstrap module's first apply (2026-08-18). Activate it once it
  shows up in `aws ce list-cost-allocation-tags`.
