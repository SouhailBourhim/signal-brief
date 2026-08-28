"""The 16:00 send. SPEC §12's 4A deliverable; ADR-0002, ADR-0010, ADR-0013.

## Why this runs locally rather than in a Lambda

ADR-0002 splits the runtime: ingestion is serverless because it has to run whether or not
the laptop is on, and everything interpretive is local. The renderer is local and holds the
finished HTML in memory, so a Lambda mailer would exist only to re-read from S3 what the
process that called it just produced — and would put the daily send behind a deployment
cycle. SPEC §13's repository layout agrees: `brief/ # ranker, renderer, mailer`.

That half of ADR-0010 stands. The transport below is the half that did not.

## SMTP through Gmail, not SES — and this reverses ADR-0010 §2

ADR-0010 chose SES because the credentials were already here: `ops/athena.py` authenticates
every brief query with `~/.aws`, so SES added an IAM action rather than a secret. The
reasoning was sound and the mail still never arrived.

Measured 2026-08-28, after five briefs the reader never saw: **SES accepted every one of
them** — `get-send-statistics` reported 8 delivery attempts, 0 bounces, 0 rejects, 0
complaints, the account `HEALTHY` with an empty suppression list — and Gmail filed every one
as Spam.

The cause is DMARC alignment, not reputation. The `From:` header claims `gmail.com` while
SES's envelope sender is `amazonses.com`, so SPF does not align; Easy DKIM on the
email-address identity was never enabled (`SigningEnabled: false`, `Status: NOT_STARTED`), so
nothing carried a signature aligned to `gmail.com` either. Both DMARC paths fail. And
`_dmarc.gmail.com` publishes `p=none; sp=quarantine`, which is precisely why nothing ever
bounced: Gmail quarantines rather than rejects, and a quarantined message still returns a
MessageId. **A green `mail` task proved the API call had been accepted, never that anything
was delivered** — the same collapse of distinct outcomes that `FetchOutcome` exists to
prevent on the ingestion side.

No SES setting fixes this for a `@gmail.com` sender. SPF and DKIM alignment for `gmail.com`
require sending through Google, and the SES sandbox additionally requires the `From:` to be a
verified identity — of which this account has exactly one, the Gmail address. Short of buying
a domain, sending through Google is the only thing that aligns. So that is what this does.

## The app password lives in SSM, which is what makes SMTP affordable

ADR-0010's objection to SMTP was never the protocol; it was the credential — "a long-lived
credential living somewhere a daily job can read it", in a project whose CI deliberately holds
no static keys (ADR-0005). One phase later `sources/macro.py` answered that exact question for
the FRED key, and the answer transfers: SSM Parameter Store, `SecureString`, encrypted at rest
under the AWS-managed key, IAM-gated, CloudTrail-audited, and read with the same `~/.aws`
credentials this module already had. The secret never touches `.env`, the repo, or Terraform
state. `infra/terraform/main/mail.tf` owns the parameter's existence and never its value.
"""

from __future__ import annotations

import smtplib
from contextlib import nullcontext
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Any

from signal_core.config import Settings
from signal_core.timeutil import brief_date

settings = Settings()

# Matches the value `mail.tf` creates the parameter with, so the failure below can say
# "still holds the Terraform placeholder" rather than Gmail's opaque authentication error.
# The same courtesy `sources/macro.py::PLACEHOLDER` extends for the FRED key.
PLACEHOLDER = "UNSET"

# Long enough to survive a slow handshake, short enough that a hung submission fails inside
# the DAG's own retry window (`brief_dag.py` retries twice, five minutes apart) rather than
# holding a task open indefinitely.
SMTP_TIMEOUT = 30

_PASSWORD_CACHE: dict[str, str] = {}


def _app_password(parameter: str) -> str:
    """The Gmail app password, from SSM Parameter Store.

    Modelled on `sources/macro.py::_api_key`, including the lazy `import boto3` that
    `ops/athena.py::_athena_client` established — every test injects the password directly
    and none of them should need botocore imported to do it.
    """
    if parameter in _PASSWORD_CACHE:
        return _PASSWORD_CACHE[parameter]

    import boto3

    try:
        response = boto3.client("ssm", region_name=settings.aws_region).get_parameter(
            Name=parameter, WithDecryption=True
        )
        value = response["Parameter"]["Value"]
    except Exception as exc:
        # Deliberately broad, for `macro.py::_api_key`'s reason: a missing parameter, a denied
        # decrypt and an SSM outage are three AWS exception types and one fact to the caller —
        # the password could not be read, so there is nothing to authenticate with.
        raise LookupError(
            f"could not read {parameter} from SSM: {type(exc).__name__}: {exc}"
        ) from exc

    if not value or value == PLACEHOLDER:
        # The expected state between `terraform apply` and the manual `put-parameter`. Saying
        # so precisely is the difference between a five-second fix and a debugging session,
        # because Gmail answers a wrong password with a bare 535.
        raise LookupError(
            f"{parameter} still holds the Terraform placeholder — set the real value with "
            f"`aws ssm put-parameter --name {parameter} --type SecureString "
            "--value <app password> --overwrite`. Generate one at "
            "https://myaccount.google.com/apppasswords (2-Step Verification must be on)."
        )

    _PASSWORD_CACHE[parameter] = value
    return value


def _compose(html: str, day: str, sender: str, recipient: str) -> EmailMessage:
    """Assemble the message. Returns it with its `Message-ID` already set.

    `EmailMessage` rather than the hand-built dict SES took, because SMTP submits a complete
    MIME document rather than a body plus metadata. That is a transport consequence, not a
    change of mind about the brief: `render.py` still produces one self-contained HTML
    document with inline CSS and no attachments.
    """
    message = EmailMessage()
    # The subject carries the date because the reader's evidence for SPEC §1's behavioural
    # criterion is a run of mornings in an inbox, and a subject line that is identical every
    # day makes that run unreadable.
    message["Subject"] = f"Signal Brief — {day}"
    message["From"] = sender
    message["To"] = recipient
    # Generated here, not assigned by a server. SMTP submission returns nothing to identify
    # the message by, so the id this function returns is a header that is actually present in
    # the delivered mail — which is more use for tracing one than an SES receipt ever was.
    # `domain=` is explicit so the id carries the sender's domain rather than leaking the
    # laptop's hostname, which is what `make_msgid()` would default to.
    message["Message-ID"] = make_msgid(domain=sender.rpartition("@")[2] or "localhost")

    # A one-line text part, not a second renderer. ADR-0010 declined a plaintext edition
    # because "a text rendering of a ranked, linked, entity-chipped page would be a second
    # renderer to keep in step with the first", and that still holds — this is a pointer.
    # It exists because `add_alternative` needs a `set_content` ahead of it to produce
    # `multipart/alternative` at all, and because an HTML-only body is itself a mild spam
    # signal, which is the thing this module is now in the business of avoiding.
    message.set_content(f"Signal Brief for {day}. This message is HTML; see the HTML part.")
    message.add_alternative(html, subtype="html")
    return message


def send_brief(
    html: str,
    *,
    date: str | None = None,
    from_addr: str | None = None,
    to_addr: str | None = None,
    password: str | None = None,
    client: Any | None = None,
) -> str:
    """Send one rendered brief. Returns the message id it was sent under.

    `password` and `client` are the injection seams, in the shape `macro.py::_api_key`
    already uses for the FRED key — supplied directly by tests, read from SSM in the DAG.
    """
    day = date or brief_date()
    sender = from_addr or settings.mail_from
    recipient = to_addr or settings.mail_to
    if not sender or not recipient:
        raise ValueError(
            "mail_from and mail_to must be set (SIGNAL_MAIL_FROM / SIGNAL_MAIL_TO, "
            "defaulting to contact_email)"
        )

    # Resolved before the socket is opened, so an unset parameter fails without a connection
    # and a login attempt standing between the operator and the message that explains it.
    secret = password or _app_password(settings.smtp_password_parameter)
    message = _compose(html, day, sender, recipient)

    # One code path for both the real connection and an injected transport, so the tests
    # exercise the same STARTTLS-then-login-then-send sequence that runs in the DAG rather
    # than a shortcut around it.
    connection = (
        nullcontext(client)
        if client is not None
        else smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=SMTP_TIMEOUT)
    )
    with connection as smtp:
        smtp.starttls()
        smtp.login(sender, secret)
        smtp.send_message(message)

    return str(message["Message-ID"])


def send_brief_file(
    path: Path,
    *,
    date: str | None = None,
    password: str | None = None,
    client: Any | None = None,
) -> str:
    """Send an already-rendered brief from disk.

    The DAG's mail task uses this rather than re-running the build: the build task already
    wrote the file and returned its path, and re-rendering to send would mean the emailed
    brief could differ from the one `make brief` opens.
    """
    return send_brief(
        path.read_text(encoding="utf-8"),
        date=date or path.stem.removeprefix("brief-"),
        password=password,
        client=client,
    )
