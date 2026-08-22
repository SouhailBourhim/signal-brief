# Phase 4A: the 07:00 send. SPEC §12; ADR-0002, ADR-0010.
#
# The mailer runs **locally**, not in Lambda. ADR-0002 puts ingestion in AWS because it must
# run whether or not the laptop is on, and everything interpretive locally; the renderer is
# local and holds the finished HTML in memory, so a Lambda would exist only to re-read from
# S3 what the process that called it just produced. What AWS provides here is the transport
# and one IAM grant, nothing else.

# The sending identity. Sender and recipient are the same address on purpose: this is a brief
# with one reader.
#
# That is also why the account stays in the **SES sandbox**. Leaving it means asking AWS for
# production access, which grants the ability to mail strangers — a capability this system
# has no use for and should not hold. In the sandbox, both ends of every send must be
# verified identities, which for a self-addressed daily brief is exactly one.
resource "aws_ses_email_identity" "brief_sender" {
  email = var.contact_email

  # AWS emails a confirmation link and Terraform cannot click it. It also cannot tell a
  # pending identity from a verified one — the same blind spot `monitoring.tf` documents for
  # the SNS subscription, and the same consequence: applying this cleanly proves nothing.
  #
  # Verify by hand, once:
  #
  #   aws sesv2 get-email-identity --email-identity <address> \
  #     --query 'VerifiedForSendingStatus'
  #
  # Until that returns `true`, `brief/mailer.py::send_brief` fails with SES's own message,
  # which names the address and is clearer than anything the code could add.
}

# A dedicated least-privilege role, mirroring `signal-analyst` in query.tf.
#
# Worth being explicit that this is not strictly necessary: the admin user (ADR-0005) already
# holds AdministratorAccess and could send today with no Terraform at all. It exists for the
# reason query.tf gives for the analyst role — "'I query with an admin key' undoes
# least-privilege even when the identity behind it is trustworthy" — and the argument is
# stronger here, because unlike a query, sending mail has an outward-facing side effect.
resource "aws_iam_role" "mailer" {
  name = "${var.name_prefix}-mailer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/${var.admin_iam_user_name}" }
    }]
  })
}

data "aws_iam_policy_document" "mailer" {
  statement {
    sid = "SendTheDailyBrief"

    # `SendEmail` alone would do: the brief is one self-contained HTML document with inline
    # CSS and no attachments, so `brief/mailer.py` never assembles MIME. `SendRawEmail` is
    # granted for headroom — an attached PDF edition is the obvious next ask — and both are
    # scoped to the one identity below, which is the constraint that actually matters.
    actions = ["ses:SendEmail", "ses:SendRawEmail"]

    resources = [aws_ses_email_identity.brief_sender.arn]
  }
}

resource "aws_iam_role_policy" "mailer" {
  name   = "${var.name_prefix}-mailer"
  role   = aws_iam_role.mailer.id
  policy = data.aws_iam_policy_document.mailer.json
}

output "mailer_role_arn" {
  description = "Assume this to send the brief; see brief/mailer.py."
  value       = aws_iam_role.mailer.arn
}

output "ses_identity_verification_check" {
  description = "Run this after apply — Terraform cannot confirm the identity itself."
  value       = "aws sesv2 get-email-identity --email-identity ${var.contact_email} --query VerifiedForSendingStatus"
}
