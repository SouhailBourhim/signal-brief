"""Time handling. SPEC §6.2: trust no timestamp.

`fetched_at` is certain — we observed it. `published_at` is claimed by the source and is
routinely wrong or absent in RSS. They are stored separately and never reconciled
silently; where they disagree beyond a threshold the article is flagged for review.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

BRIEF_TZ = ZoneInfo("Africa/Casablanca")

# Beyond this, a source's claimed publication time is not credible enough to rank on.
TIMESTAMP_DISAGREEMENT = timedelta(hours=48)


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Attach UTC to naive datetimes rather than guessing a local zone.

    Guessing is how a pipeline silently shifts every timestamp by an hour twice a year.
    """
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def timestamps_disagree(
    fetched_at: datetime,
    published_at: datetime | None,
    threshold: timedelta = TIMESTAMP_DISAGREEMENT,
) -> bool:
    """True when `published_at` is missing, in the future, or implausibly old.

    A future `published_at` is always a lie: we cannot have fetched it before it existed.
    """
    if published_at is None:
        return True
    published_at = ensure_utc(published_at)
    fetched_at = ensure_utc(fetched_at)
    if published_at > fetched_at + timedelta(minutes=5):
        return True
    return fetched_at - published_at > threshold


def brief_date(moment: datetime | None = None) -> str:
    """The brief's date label, in the reader's timezone rather than UTC.

    The brief is a local artifact for a reader in Casablanca; labelling a 16:00 local
    edition with a UTC date would name some editions after the previous day.
    """
    return ensure_utc(moment or utc_now()).astimezone(BRIEF_TZ).strftime("%Y-%m-%d")


def ingest_partition(fetched_at: datetime) -> tuple[str, str]:
    """(ingest_date, hour) partition values for the bronze layout. SPEC §6.4."""
    m = ensure_utc(fetched_at)
    return m.strftime("%Y-%m-%d"), m.strftime("%H")
