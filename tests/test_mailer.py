"""The 16:00 send. 4A.J; SPEC §12, ADR-0010, ADR-0013.

These were written against `moto`'s SES mock until 2026-08-28, when five briefs that SES
had accepted with zero bounces turned out to have been sitting in Gmail's Spam folder the
whole time. `moto` was mocking the wrong thing — not incorrectly, but at a layer where a
message that would never be delivered still looks like a success. ADR-0013 has the
measurement; the tests below now drive an injected SMTP transport instead, and assert the
submission sequence rather than an API acknowledgement.

Still no network: the transport is a recording stub, and the SSM tests use `moto`.
"""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from signal_core.brief import mailer
from signal_core.brief.mailer import PLACEHOLDER, send_brief, send_brief_file

ADDRESS = "reader@example.com"
PASSWORD = "abcdefghijklmnop"
HTML = "<html><body><h1>Signal Brief</h1><p>Northwind acquires Lumen Robotics</p></body></html>"
PARAMETER = "/signal/gmail-app-password"


class _Transport:
    """Records the submission sequence without opening a socket.

    `send_brief` drives this through the identical `starttls` → `login` → `send_message`
    path it drives a real `smtplib.SMTP` through, so these tests cover the sequence rather
    than a shortcut around it.
    """

    def __init__(self) -> None:
        self.started_tls = False
        self.credentials: tuple[str, str] | None = None
        self.messages: list = []

    def starttls(self) -> None:
        self.started_tls = True

    def login(self, user: str, password: str) -> None:
        assert self.started_tls, "credentials must not cross the wire before STARTTLS"
        self.credentials = (user, password)

    def send_message(self, message) -> None:
        assert self.credentials is not None, "sent before authenticating"
        self.messages.append(message)


@pytest.fixture
def transport() -> _Transport:
    return _Transport()


@pytest.fixture(autouse=True)
def _clear_password_cache():
    mailer._PASSWORD_CACHE.clear()
    yield
    mailer._PASSWORD_CACHE.clear()


def _send(transport: _Transport, **kwargs) -> str:
    defaults = {
        "date": "2026-08-22",
        "from_addr": ADDRESS,
        "to_addr": ADDRESS,
        "password": PASSWORD,
        "client": transport,
    }
    return send_brief(HTML, **{**defaults, **kwargs})


def test_a_brief_is_sent(transport):
    message_id = _send(transport)

    assert len(transport.messages) == 1
    assert message_id
    # The id is a header that is actually present in the delivered mail, not a receipt from
    # a server — which is the point of generating it locally (ADR-0013).
    assert transport.messages[0]["Message-ID"] == message_id


def test_the_subject_carries_the_date(transport):
    """SPEC §1's criterion is behavioural and its evidence is a run of mornings in an inbox.
    An identical subject line every day makes that run unreadable."""
    _send(transport)
    assert transport.messages[0]["Subject"] == "Signal Brief — 2026-08-22"


def test_the_html_is_carried_intact_with_a_plain_text_alternative(transport):
    """The inverse of what this file asserted before ADR-0013.

    SES took a body plus metadata and HTML-only was the tighter grant. SMTP submits a
    complete MIME document, and an HTML-only body is a mild spam signal — the exact class of
    problem this transport exists to avoid. The text part is a one-line pointer, deliberately
    not a second rendering of a ranked, linked, entity-chipped page.
    """
    _send(transport)
    message = transport.messages[0]

    assert message.get_content_type() == "multipart/alternative"

    html_part = message.get_body(("html",))
    assert html_part is not None
    assert html_part.get_content().strip() == HTML

    text_part = message.get_body(("plain",))
    assert text_part is not None
    assert "2026-08-22" in text_part.get_content()
    assert "Northwind" not in text_part.get_content()


def test_the_submission_authenticates_over_tls_as_the_sender(transport):
    """Gmail aligns SPF and DKIM because the message is submitted by the account that owns
    the `From:` address. Logging in as anyone else would put the alignment back where SES
    left it (ADR-0013)."""
    _send(transport)
    assert transport.started_tls
    assert transport.credentials == (ADDRESS, PASSWORD)


def test_the_message_id_carries_the_sender_domain_not_the_hostname(transport):
    """`make_msgid()` defaults to the local hostname, which on this project is a WSL box.
    The domain is pinned so the id says `gmail.com` rather than leaking the laptop's name."""
    message_id = _send(transport)
    assert message_id.endswith("@example.com>")


def test_missing_addresses_raise_before_reaching_smtp(monkeypatch, transport):
    """Only reachable when `contact_email` itself is empty, since `mail_from`/`mail_to`
    fall back to it — but that is a real misconfiguration, and failing here names it
    instead of opening a connection to authenticate an empty sender."""
    from signal_core.config import Settings

    monkeypatch.setattr(mailer, "settings", Settings(contact_email=""))
    with pytest.raises(ValueError, match="mail_from and mail_to"):
        send_brief(HTML, date="2026-08-22", password=PASSWORD, client=transport)
    assert transport.messages == []


def test_sending_a_rendered_file_takes_its_date_from_the_filename(tmp_path: Path, transport):
    """The DAG mails the file `build` wrote rather than re-rendering: the reader and the
    record have to be the same artifact."""
    path = tmp_path / "brief-2026-08-22.html"
    path.write_text(HTML, encoding="utf-8")

    send_brief_file(path, password=PASSWORD, client=transport)
    message = transport.messages[0]
    assert message["Subject"] == "Signal Brief — 2026-08-22"
    assert message.get_body(("html",)).get_content().strip() == HTML


def test_the_addresses_default_to_contact_email():
    """The brief has one reader who is also its sender, and that only works if both ends
    default to the same address — which since ADR-0013 is also what aligns SPF and DKIM."""
    from signal_core.config import Settings

    settings = Settings(contact_email="someone@example.com")
    assert settings.mail_from == "someone@example.com"
    assert settings.mail_to == "someone@example.com"


# --- the app password ----------------------------------------------------------------------


@mock_aws
def test_the_app_password_is_read_from_ssm():
    client = boto3.client("ssm", region_name="us-east-1")
    client.put_parameter(Name=PARAMETER, Value=PASSWORD, Type="SecureString")

    assert mailer._app_password(PARAMETER) == PASSWORD


@mock_aws
def test_the_terraform_placeholder_names_the_fix():
    """The expected state between `terraform apply` and the manual `put-parameter`. Gmail
    answers a wrong password with a bare 535, so saying this here is the difference between
    a five-second fix and a debugging session — `sources/macro.py`'s courtesy for the FRED
    key, which this reuses."""
    client = boto3.client("ssm", region_name="us-east-1")
    client.put_parameter(Name=PARAMETER, Value=PLACEHOLDER, Type="SecureString")

    with pytest.raises(LookupError, match="Terraform placeholder") as failure:
        mailer._app_password(PARAMETER)
    assert PARAMETER in str(failure.value)
    assert "put-parameter" in str(failure.value)


@mock_aws
def test_an_unreadable_parameter_is_one_fact_to_the_caller():
    """A missing parameter, a denied decrypt and an SSM outage are three AWS exception types
    and one fact: there is nothing to authenticate with."""
    with pytest.raises(LookupError, match="could not read"):
        mailer._app_password("/signal/does-not-exist")
