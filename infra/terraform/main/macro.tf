# Phase 4B: the FRED/ALFRED key. SPEC §8; docs/runbooks/phase-4b.md 4B.H.
#
# This is the **first secret in the project**. Every source before it authenticates with
# nothing, or with a User-Agent that identifies the operator rather than proving anything.
# FRED requires a real key — confirmed against the live endpoint before the poller was
# written, which answers HTTP 400 with `Variable api_key is not set` to an anonymous request.

# SSM Parameter Store, not a Lambda environment variable, and not Secrets Manager.
#
# **Not an environment variable**, which was the tempting option: it costs no new IAM, no
# extra API call, and one line in `lambda.tf`'s existing `environment` block. It would also
# put the key in Terraform state — which lives in an S3 bucket — and render it in plaintext
# in the Lambda console to anyone with read access to the function. For the saving of one
# `GetParameter` at cold start, that is a bad trade.
#
# **Not Secrets Manager**, which is the purpose-built service: it charges $0.40 per secret
# per month plus API calls, against $0 here. Its distinguishing features are rotation and
# cross-account sharing, and a free personal API key needs neither. SPEC §10 treats staying
# inside the free tier as a design constraint rather than a preference, so the paid service
# has to earn the line item, and it does not.
#
# A `SecureString` under the AWS-managed `alias/aws/ssm` key is encrypted at rest and carries
# no monthly charge — only a customer-managed CMK would, at $1/month.
resource "aws_ssm_parameter" "fred_api_key" {
  name        = "/signal/fred-api-key"
  description = "FRED/ALFRED API key. Terraform owns this parameter's existence, never its value."
  type        = "SecureString"

  # Terraform creates the parameter and never learns the secret. Putting the real key in a
  # `.tfvars` would defeat the entire reason for not using an environment variable, since it
  # would land in state either way.
  #
  # `sources/macro.py::PLACEHOLDER` matches this string, so the poller can say precisely
  # "still holds the Terraform placeholder" rather than the useless "unauthorized" FRED
  # would return — the same courtesy `mail.tf` extends for its unverified SES identity.
  value = "UNSET"

  lifecycle {
    # Without this, every `terraform apply` after the key is set would helpfully reset it
    # back to the placeholder and break ingestion until someone noticed.
    ignore_changes = [value]
  }
}

# Set the real value by hand, once — the same shape as `mail.tf`'s manual SES verification,
# where Terraform provisions the thing and a human completes it:
#
#   aws ssm put-parameter --name /signal/fred-api-key --type SecureString \
#     --value <key from https://fredaccount.stlouisfed.org/apikeys> --overwrite
#
# Confirm with:
#
#   aws ssm get-parameter --name /signal/fred-api-key --with-decryption \
#     --query 'Parameter.Value' --output text

# The poller role gains exactly one parameter and one key. Scoped to the parameter ARN rather
# than `/signal/*`: a wildcard here would silently grant every future secret in this
# namespace to eight Lambdas that have no business reading them.
data "aws_iam_policy_document" "macro_secret" {
  statement {
    sid       = "ReadFredApiKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.fred_api_key.arn]
  }

  statement {
    sid     = "DecryptWithSsmManagedKey"
    effect  = "Allow"
    actions = ["kms:Decrypt"]

    # `"*"` here is narrower than it looks, and the alternative is worse. `kms:Decrypt`
    # requires a *key* ARN as its resource; an alias ARN like `alias/aws/ssm` is not accepted
    # and silently matches nothing. The AWS-managed key behind Parameter Store has an
    # account-specific id that Terraform would have to look up and that AWS may rotate, so
    # pinning it makes the policy fragile for no security gain.
    #
    # The `ViaService` condition below is what actually constrains this: the grant is
    # unusable except through Parameter Store in this region, which combined with the
    # single-parameter `GetParameter` grant above means it can decrypt exactly one secret.
    # This is AWS's own documented pattern for AWS-managed keys.
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "macro_secret" {
  name   = "${var.name_prefix}-macro-secret"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.macro_secret.json
}

output "fred_api_key_check" {
  description = "Confirm the key is set — a value of UNSET means ingestion will fail with a clear message."
  value       = "aws ssm get-parameter --name ${aws_ssm_parameter.fred_api_key.name} --with-decryption --query 'Parameter.Value' --output text"
}
