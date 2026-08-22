"""The 07:00 send. SPEC §12's 4A deliverable; ADR-0002, ADR-0010.

## Why this runs locally rather than in a Lambda

ADR-0002 splits the runtime: ingestion is serverless because it has to run whether or not
the laptop is on, and everything interpretive is local. The renderer is local and holds the
finished HTML in memory, so a Lambda mailer would exist only to re-read from S3 what the
process that called it just produced — and would put the daily send behind a deployment
cycle. SPEC §13's repository layout agrees: `brief/ # ranker, renderer, mailer`.

## SES rather than SMTP

The credentials are already here. `ops/athena.py` authenticates every brief query with
`~/.aws`, so the mailer adds an IAM action rather than a secret. A Gmail app password would
be a long-lived credential living somewhere a daily job can read it, in a project whose CI
deliberately holds no static keys (ADR-0005).

`send_email` with an HTML body, not `send_raw_email`: the brief is one self-contained
document with inline CSS and no attachments (`render.py`), so MIME assembly would be
ceremony around a string that is already complete.

## The identity has to be verified by hand, once

Terraform declares `aws_ses_email_identity` and AWS emails a confirmation link. Terraform
cannot click it, and — exactly like the SNS subscription `monitoring.tf` already documents —
it cannot tell a pending identity from a verified one. `infra/terraform/main/mail.tf`
carries the check. Until it is done, `send_brief` fails with SES's own message, which names
the address and is clearer than anything this module could add.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from signal_core.config import Settings
from signal_core.timeutil import brief_date

settings = Settings()


def _ses(client: Any | None = None) -> Any:
    """Lazily, matching `ops/athena.py::_athena_client` — importing boto3 at module scope
    would put it in the import chain of anything that imports the brief package."""
    if client is not None:
        return client
    import boto3

    return boto3.client("ses", region_name=settings.aws_region)


def send_brief(
    html: str,
    *,
    date: str | None = None,
    from_addr: str | None = None,
    to_addr: str | None = None,
    client: Any | None = None,
) -> str:
    """Send one rendered brief. Returns SES's message id.

    The subject carries the date because the reader's evidence for SPEC §1's behavioural
    criterion is a run of mornings in an inbox, and a subject line that is identical every
    day makes that run unreadable.
    """
    day = date or brief_date()
    sender = from_addr or settings.mail_from
    recipient = to_addr or settings.mail_to
    if not sender or not recipient:
        raise ValueError(
            "mail_from and mail_to must be set (SIGNAL_MAIL_FROM / SIGNAL_MAIL_TO, "
            "defaulting to contact_email)"
        )

    response = _ses(client).send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": f"Signal Brief — {day}", "Charset": "UTF-8"},
            # HTML only, no plaintext alternative. A text rendering of a ranked, linked,
            # entity-chipped page would be a second renderer to keep in step with the first,
            # and this has exactly one reader, whose client renders HTML.
            "Body": {"Html": {"Data": html, "Charset": "UTF-8"}},
        },
    )
    return response.get("MessageId", "")


def send_brief_file(
    path: Path,
    *,
    date: str | None = None,
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
        client=client,
    )
