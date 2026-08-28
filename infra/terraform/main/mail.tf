# Phase 4A: the 16:00 send. SPEC §12; ADR-0002, ADR-0010, ADR-0013.
#
# The mailer runs **locally**, not in Lambda. ADR-0002 puts ingestion in AWS because it must
# run whether or not the laptop is on, and everything interpretive locally; the renderer is
# local and holds the finished HTML in memory, so a Lambda would exist only to re-read from
# S3 what the process that called it just produced. That decision stands.
#
# What changed on 2026-08-28 is the transport, and this file is most of the evidence for why.
#
# ## What used to be here, and why it is gone
#
# This file declared an `aws_ses_email_identity` for the reader's Gmail address and a
# least-privilege `signal-mailer` role holding `ses:SendEmail` scoped to it. Both applied
# cleanly. The identity verified. The role worked. Five briefs went out and the reader saw
# none of them:
#
#   aws ses get-send-statistics   -> 8 delivery attempts, 0 bounces, 0 rejects, 0 complaints
#   aws sesv2 get-account         -> SendingEnabled true, HEALTHY, suppression list empty
#   aws sesv2 get-email-identity  -> VerifiedForSendingStatus true
#                                    DkimAttributes.SigningEnabled FALSE, Status NOT_STARTED
#
# Every brief was in Gmail's Spam folder. The `From:` header claimed `gmail.com` while the
# envelope sender was `amazonses.com`, so SPF did not align, and with Easy DKIM never enabled
# on an email-address identity nothing was signed for `gmail.com` either — so DMARC failed
# both ways. `_dmarc.gmail.com` publishes `p=none; sp=quarantine`, which is exactly why it
# never bounced: Gmail quarantines instead of rejecting, and returns a MessageId while doing
# it.
#
# **No SES configuration fixes that for a `@gmail.com` sender.** Aligning SPF and DKIM for
# `gmail.com` means sending through Google, and the sandbox additionally requires the `From:`
# to be a verified identity — of which this account has exactly one, that same Gmail address.
# The fix is a domain (then a domain identity, Easy DKIM, and a custom MAIL FROM) or Google's
# own submission service. ADR-0013 takes the second, and `brief/mailer.py` is now SMTP.
#
# The SES identity and role are therefore deleted rather than left dormant: dead
# infrastructure that once looked like it worked is worse than none, and re-verifying an
# address is one click if a domain is ever bought.

# The Gmail app password, in the shape `macro.tf` established for the first secret.
#
# This is what makes SMTP affordable. ADR-0010's objection to it was never the protocol but
# the credential — "a long-lived credential living somewhere a daily job can read it", in a
# project whose CI deliberately holds no static keys (ADR-0005). Parameter Store answers that
# the same way it answered it for the FRED key: a `SecureString` under the AWS-managed
# `alias/aws/ssm` key is encrypted at rest, IAM-gated, CloudTrail-audited, and carries no
# monthly charge. The password never touches `.env`, the repo, or Terraform state, and it
# reaches the mailer over the same `~/.aws` credentials `ops/athena.py` already uses — which
# was the whole reason SES won the argument in the first place.
#
# Not Secrets Manager, for `macro.tf`'s reason: $0.40/secret/month buys rotation and
# cross-account sharing, and a Gmail app password needs neither. SPEC §10 treats the free
# tier as a design constraint.
resource "aws_ssm_parameter" "gmail_app_password" {
  name        = "/signal/gmail-app-password"
  description = "Gmail app password for the 16:00 brief. Terraform owns this parameter's existence, never its value."
  type        = "SecureString"

  # Terraform creates the parameter and never learns the secret. `brief/mailer.py::PLACEHOLDER`
  # matches this string so the mailer can say "still holds the Terraform placeholder" rather
  # than surfacing Gmail's bare 535.
  value = "UNSET"

  lifecycle {
    # Without this, every `terraform apply` after the password is set would helpfully reset it
    # to the placeholder and break the send until someone noticed — which, on the evidence of
    # this file's history, could take five days.
    ignore_changes = [value]
  }
}

# Set the real value by hand, once — the same shape as the SES verification this replaces,
# where Terraform provisions the thing and a human completes it. The app password requires
# 2-Step Verification on the Google account; Google will not offer the option otherwise.
#
#   aws ssm put-parameter --name /signal/gmail-app-password --type SecureString \
#     --value <16 chars from https://myaccount.google.com/apppasswords> --overwrite
#
# No IAM role accompanies this one, and the omission is deliberate rather than an oversight.
# `macro.tf` needed a policy because the reader is a Lambda running as `signal-poller`. The
# mailer runs locally as `Souhail_Signal_Admin`, which already holds `AdministratorAccess`
# (ADR-0005) — so a role here would have to be assumed by hand before every send to grant
# what the caller already has. `query.tf`'s argument for `signal-analyst` does not carry over
# either: that role exists to keep an *interactive* human out of an admin key, and this is an
# unattended daily job with one action.

output "gmail_app_password_check" {
  description = "Confirm the password is set — a value of UNSET means the 16:00 send will fail with a clear message."
  value       = "aws ssm get-parameter --name ${aws_ssm_parameter.gmail_app_password.name} --with-decryption --query 'Parameter.Value' --output text"
}
