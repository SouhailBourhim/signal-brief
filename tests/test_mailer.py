"""The 07:00 send. 4A.J; SPEC §12, ADR-0010.

`moto`'s `mock_aws` covers `ses.send_email` with no dependency change — verified against the
installed environment rather than assumed, since the whole reason SES won over SMTP was that
it needs no new credential.
"""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from signal_core.brief.mailer import send_brief, send_brief_file

ADDRESS = "reader@example.com"
HTML = "<html><body><h1>Signal Brief</h1><p>Northwind acquires Lumen Robotics</p></body></html>"


@pytest.fixture
def ses():
    with mock_aws():
        client = boto3.client("ses", region_name="us-east-1")
        client.verify_email_identity(EmailAddress=ADDRESS)
        yield client


def _sent(client):
    return client.get_send_statistics(), client


def test_a_brief_is_sent_as_html(ses):
    message_id = send_brief(HTML, date="2026-08-22", from_addr=ADDRESS, to_addr=ADDRESS, client=ses)
    assert message_id

    quota = ses.get_send_quota()
    assert quota["SentLast24Hours"] == 1.0


def test_the_subject_carries_the_date(ses):
    """SPEC §1's criterion is behavioural and its evidence is a run of mornings in an inbox.
    An identical subject line every day makes that run unreadable."""
    captured = {}

    class _Recording:
        def send_email(self, **kwargs):
            captured.update(kwargs)
            return {"MessageId": "m1"}

    send_brief(HTML, date="2026-08-22", from_addr=ADDRESS, to_addr=ADDRESS, client=_Recording())
    assert captured["Message"]["Subject"]["Data"] == "Signal Brief — 2026-08-22"


def test_the_body_is_html_not_raw_mime(ses):
    """The brief is one self-contained document with inline CSS and no attachments
    (`render.py`), so MIME assembly would be ceremony around a complete string — and
    `ses:SendEmail` is a tighter grant than `SendRawEmail`."""
    captured = {}

    class _Recording:
        def send_email(self, **kwargs):
            captured.update(kwargs)
            return {"MessageId": "m1"}

    send_brief(HTML, date="2026-08-22", from_addr=ADDRESS, to_addr=ADDRESS, client=_Recording())
    body = captured["Message"]["Body"]
    assert "Html" in body and "Text" not in body
    assert body["Html"]["Data"] == HTML
    assert body["Html"]["Charset"] == "UTF-8"


def test_an_unverified_sender_fails_loudly(ses):
    """SES's own error names the address, which is more useful than anything this module
    could add — and the identity being unverified is the expected first-run state
    (`infra/terraform/main/mail.tf`)."""
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as failure:
        send_brief(
            HTML,
            date="2026-08-22",
            from_addr="nobody@example.com",
            to_addr=ADDRESS,
            client=ses,
        )
    assert "nobody@example.com" in str(failure.value)


def test_missing_addresses_raise_before_reaching_ses(monkeypatch):
    """Only reachable when `contact_email` itself is empty, since `mail_from`/`mail_to`
    fall back to it — but that is a real misconfiguration, and failing here names it
    instead of letting SES reject an empty Source."""
    from signal_core.brief import mailer
    from signal_core.config import Settings

    monkeypatch.setattr(mailer, "settings", Settings(contact_email=""))
    with pytest.raises(ValueError, match="mail_from and mail_to"):
        send_brief(HTML, date="2026-08-22", client=object())


def test_sending_a_rendered_file_takes_its_date_from_the_filename(tmp_path: Path, ses):
    """The DAG mails the file `build` wrote rather than re-rendering: the reader and the
    record have to be the same artifact."""
    path = tmp_path / "brief-2026-08-22.html"
    path.write_text(HTML, encoding="utf-8")

    captured = {}

    class _Recording:
        def send_email(self, **kwargs):
            captured.update(kwargs)
            return {"MessageId": "m1"}

    send_brief_file(path, client=_Recording())
    assert captured["Message"]["Subject"]["Data"] == "Signal Brief — 2026-08-22"
    assert captured["Message"]["Body"]["Html"]["Data"] == HTML


def test_the_addresses_default_to_contact_email():
    """The SES sandbox is the right default because the brief has one reader who is also its
    sender, and that only works if both ends default to the same address (ADR-0010)."""
    from signal_core.config import Settings

    settings = Settings(contact_email="someone@example.com")
    assert settings.mail_from == "someone@example.com"
    assert settings.mail_to == "someone@example.com"
