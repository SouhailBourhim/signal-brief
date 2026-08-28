# ADR-0013 — The brief is submitted through Gmail, not SES

**Status:** Accepted · **Date:** 2026-08-28 · **Supersedes ADR-0010 §2**

## Context

ADR-0010 §2 decided that the brief would be mailed from the local side via SES, and gave two
reasons. The first — *mail from the local side*, because the renderer holds the finished HTML
in memory and a Lambda mailer would only re-read from S3 what its caller just produced — is
untouched by this record and still correct. The second was the transport:

> SES over SMTP because the credentials are already there: `~/.aws` is what `ops/athena.py`
> authenticates every brief query with, so the mailer adds an IAM action rather than a secret.
> Gmail SMTP would need an app password — a long-lived credential in a project whose CI
> deliberately runs on OIDC with no static AWS keys (ADR-0005), stored somewhere for a daily
> job to read.

That argument was sound and the mail still did not arrive. Five briefs were built, sent, and
never read, and **nothing in the system said so.** The `brief` DAG was green on 08-23, 08-24,
08-25, 08-27 and 08-28; each `mail` task returned a real SES MessageId into XCom.

The reader reported it as "the email is not being sent." It was being sent. What follows is
what the account actually said when it was asked:

| Question | Answer |
|---|---|
| `aws ses get-send-statistics` | 8 delivery attempts, **0 bounces, 0 rejects, 0 complaints** |
| `aws sesv2 get-account` | `SendingEnabled: true`, `EnforcementStatus: HEALTHY` |
| account suppression list | empty |
| `aws sesv2 get-email-identity` | `VerifiedForSendingStatus: true` |
| …its DKIM attributes | **`SigningEnabled: false`, `Status: NOT_STARTED`** |
| `_dmarc.gmail.com` | `v=DMARC1; p=none; sp=quarantine` |
| the reader's Spam folder | **all five briefs** |

**The defect is DMARC alignment.** The `From:` header claims `gmail.com`. SES's envelope
sender is `amazonses.com`, so SPF authenticates a domain that is not the one in the header —
it does not *align*. Easy DKIM was never enabled on the identity, so nothing carried a
signature aligned to `gmail.com` either. DMARC requires one of the two to align and neither
did.

Two things about this are worth stating precisely, because both misled the diagnosis for
days:

- **Nothing bounced, and that was not a good sign.** `gmail.com` publishes `p=none`, so Gmail
  is not asked to reject on DMARC failure. It quarantines instead. A quarantined message is
  accepted at SMTP time, counts as a delivery attempt, and returns a MessageId.
- **The 07:00 → 16:00 move (ADR-0010's 2026-08-24 amendment) was a real fix for a different
  bug.** The host slept through 07:00 and the scheduler was frozen. Fixing that made the send
  happen on time and changed nothing about where it landed, which is why "my laptop is up
  now" did not help.

This is the same failure the ingestion side was designed against and the brief side was not.
`FetchOutcome` exists precisely so that `NOT_MODIFIED`, `EMPTY` and `ERROR` cannot collapse
into "0 docs" and hide a stale-but-successful source. The mailer collapsed *accepted* into
*delivered*, and hid a quarantined-but-successful send for five days.

## Decision

**Submit the brief through Gmail's own SMTP service, authenticated as the sending account,
with the app password held in SSM Parameter Store.**

### Why not fix SES instead

There is no configuration of SES that aligns `gmail.com`. SPF alignment needs an envelope
sender under `gmail.com`; DKIM alignment needs a signing key published under `gmail.com`.
Both are Google's to hold, and Google issues neither to third-party senders. The SES sandbox
further requires the `From:` to be a verified identity, and this account has exactly one — the
Gmail address itself. Enabling Easy DKIM would have signed for `amazonses.com`, which does not
align either.

### Why not buy a domain

This is the better long-term answer and it was offered and declined. A domain identity with
Easy DKIM (three CNAMEs) and a custom MAIL FROM subdomain aligns both mechanisms, keeps SES,
and would let the brief send from an address that is not also the reader's personal mailbox.
It costs a domain, DNS control, and a renewal the project has to keep paying attention to, for
a brief with one reader who already owns a mailbox that can send to itself. Recorded as the
reversing condition below rather than dismissed.

### Why not just add a Gmail filter

A "never send to spam" filter does make the mail land, and it was applied immediately as a
stopgap. It is not the fix: it lives in a Gmail account rather than the repository, no test or
`terraform plan` can see it, it does not survive being rebuilt on another account, and it
leaves the system sending mail that fails authentication — which `p=none` tolerates today and
need not tomorrow. It stays in place as belt-and-braces behind a send that no longer needs it.

### The credential objection, and what answers it

ADR-0010's resistance to SMTP was never about the protocol; it was about the app password —
"a long-lived credential living somewhere a daily job can read it." That objection was correct
when it was written and has since been answered by this project's own work. ADR-0011 and
`macro.tf` introduced the first secret, the FRED key, and settled the pattern: SSM Parameter
Store, `SecureString` under the AWS-managed key, encrypted at rest, IAM-gated, CloudTrail-
audited, free. `brief/mailer.py::_app_password` is `sources/macro.py::_api_key` with a
different parameter name, down to the placeholder message that names the exact
`put-parameter` command.

So the secret lives where the project already puts secrets, and is read with the same `~/.aws`
credentials that made SES attractive in the first place. The thing ADR-0010 valued about SES —
no new credential path — is preserved. What is given up is one AWS service, and what is bought
is mail that authenticates.

## Consequences

**The mailer sends MIME.** SES took a body plus metadata; SMTP submits a document. The module
now builds an `EmailMessage` with a one-line plaintext part and the rendered HTML as the
alternative. ADR-0010's argument against a plaintext *edition* stands — a text rendering of a
ranked, linked, entity-chipped page would be a second renderer to keep in step with the first
— so the text part is a pointer, not a rendering. It exists because `add_alternative` needs a
`set_content` before it, and because an HTML-only body is itself a mild spam signal.

**The message id is now generated locally and is worth more.** SMTP submission returns no
identifier, so `mailer.py` sets its own `Message-ID` and returns that. The DAG's XCom
therefore holds a header that is present in the delivered mail and searchable in the mailbox,
rather than an SES receipt for a message that may never have been seen.

**The SES identity and the `signal-mailer` IAM role are deleted, not left dormant.**
`terraform apply` will destroy `aws_ses_email_identity.brief_sender`, `aws_iam_role.mailer`
and its policy. Dead infrastructure that once looked like it worked is worse than none, and
re-verifying an address is one click if the domain option is ever taken. `mail.tf` keeps the
diagnosis in its header so the next reader does not re-derive it.

**No IAM role accompanies the new parameter, deliberately.** `macro.tf` needed one because its
reader is a Lambda running as `signal-poller`. The mailer runs locally as
`Souhail_Signal_Admin`, which already holds `AdministratorAccess` — a role here would have to
be assumed by hand before every send to grant what the caller already has. `query.tf`'s
argument for `signal-analyst` does not carry either: that role keeps an *interactive* human
out of an admin key, and this is an unattended job with one action.

**One manual step replaces another.** SES needed a verification link clicked once; Gmail needs
2-Step Verification enabled and an app password generated once. Terraform provisions the
parameter and never owns its value, exactly as it never owned the FRED key's.

**The tests changed layer, and that is the lesson worth keeping.** `tests/test_mailer.py` was
built on `moto`'s SES mock. It passed throughout — correctly, since the SES call really was
well-formed. It was mocking a layer at which an undeliverable message is indistinguishable
from a delivered one. The tests now drive an injected SMTP transport and assert the
STARTTLS-then-login-then-send sequence, which is closer to the wire but still not proof of
delivery. **Nothing offline is.** The check that would have caught this in five minutes is
manual and belongs in the runbook: open the delivered mail, *Show original*, and read
`SPF: PASS`, `DKIM: PASS`, `DMARC: PASS`.

**What would reverse this.** Buying a domain. A domain identity in SES with Easy DKIM and a
custom MAIL FROM aligns both mechanisms properly, removes the app password entirely, returns
the send to `~/.aws`-only credentials, and lets the brief come from an address that is not the
reader's personal mailbox. At that point ADR-0010 §2's original reasoning becomes correct
again and this record should be superseded in turn.
