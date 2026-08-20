"""Reading the lake for the daily brief. SPEC §7.1, §10.1, §12's brief ladder.

The brief is a *query*, not a transform, so it goes through Athena rather than Spark.
Three reasons, in the order they matter:

  1. **Egress.** A local Spark session reading an `s3://` warehouse pulls every projected
     byte onto this machine — SPEC §10.1's line item nobody budgets. Athena scans inside
     AWS and egresses only the result rows, which for a brief is tens of them.
  2. **The footer's cost fields.** `RunHealth.bytes_scanned` and `.estimated_cost_usd`
     have existed unpopulated since Phase 0. `QueryResult` carries exactly those two
     numbers, so reading through Athena is what finally lets the footer state what the
     brief cost to produce, instead of printing a zero SPEC §17 would call a fiction.
  3. **No JVM.** `make brief` runs every morning. Starting Spark and resolving Iceberg's
     runtime jar from Maven to render ten stories is a lot of machinery for a SELECT.

Athena returns every value as a string or None (`ops/athena.py::_collect_rows`), so the
coercion below is the whole interesting part of this module.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from signal_core.config import DEPLOYED_SOURCE_IDS, settings
from signal_core.ops.athena import QueryResult, run_query
from signal_core.ops.health import SourceHealth
from signal_core.timeutil import ensure_utc

# SPEC §7.1: same-story clustering runs over a time-decayed 72-hour window.
CLUSTER_WINDOW_HOURS = 72

# How far back to look for a health verdict per source. Longer than the article window on
# purpose: a source that stopped being assessed three days ago is exactly the one whose
# absence the footer must report, and a window that drops it would report silence as health.
HEALTH_LOOKBACK_HOURS = 168

# Only what `dedup` and `ranker` actually consume. Projection is the larger half of the
# bytes-scanned win on a columnar table — 184 KB to 73 KB on the same question, measured in
# `docs/athena.md` — and it comes before partition pruning, not after.
_ARTICLE_COLUMNS = (
    "article_id",
    "source_id",
    "url_canonical",
    "title",
    "body_text",
    "published_at",
    "fetched_at",
    "publisher_domain",
    "simhash",
    "content_hash",
    "timestamp_flagged",
    "story_key",
)

_HEALTH_COLUMNS = (
    "source_id",
    "window_start",
    "docs_ingested",
    "expected_min",
    "last_success_at",
    "staleness_seconds",
    "status",
    "gap_reason",
    "content_staleness_seconds",
    "baseline_docs",
)


def _sql_timestamp(moment: datetime) -> str:
    """Trino's timestamp literal body. Second precision is deliberate: the partition
    transform is `days(event_date)`, so sub-second bounds buy nothing and only add a way
    for a literal to fail to parse."""
    return ensure_utc(moment).strftime("%Y-%m-%d %H:%M:%S")


# Athena's Iceberg connector renders a `timestamp` column as
# `2026-08-19 12:22:49.000000 UTC` — a six-digit fraction and a trailing zone name. The
# earlier guess (`.000`, no zone) was wrong, and this is where 3.0 found out; see
# docs/runbooks/phase-3.md. Accept the shapes Athena and Trino actually emit, and read the
# zone rather than trimming it: silently discarding a `+02:00` would shift every timestamp
# in the brief by two hours, which is the exact failure `timeutil.ensure_utc` exists to
# refuse to commit.
_ZONE_SUFFIX = re.compile(r"\s*(?:(?P<name>UTC|GMT|Z)|(?P<offset>[+-]\d{2}:?\d{2}))$")

# Only the `T` that separates a date from a time. A bare `.replace("T", " ")` also eats the
# T out of the word `UTC`, turning `...49.000000 UTC` into `...49.000000 U C` and making
# the zone unmatchable — which is precisely how this function failed on its first real row.
_ISO_SEPARATOR = re.compile(r"(?<=\d)T(?=\d)")


def _parse_timestamp(value: str | None) -> datetime | None:
    """Athena timestamp string -> aware UTC datetime.

    Raises rather than returning None on an unrecognised shape. A None here would render a
    format change as a null column, and a null `published_at` is meaningful — it makes the
    ranker distrust the article and fall back to `fetched_at` (SPEC §6.2). Failing loudly
    is what turned Athena's actual rendering into a five-minute fix instead of a brief in
    which nothing had ever been published.
    """
    if value is None:
        return None
    # Zone first, separator second. The reverse order corrupts `UTC` (see _ISO_SEPARATOR).
    text = value.strip()

    offset = timedelta(0)
    if match := _ZONE_SUFFIX.search(text):
        text = text[: match.start()].strip()
        if raw := match.group("offset"):
            hours, _, minutes = raw[1:].partition(":")
            offset = timedelta(hours=int(hours), minutes=int(minutes or 0))
            if raw[0] == "-":
                offset = -offset

    text = _ISO_SEPARATOR.sub(" ", text, count=1)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            # Subtracting the offset converts the local reading to UTC: 14:00+02:00 is
            # 12:00Z.
            return datetime.strptime(text, fmt).replace(tzinfo=UTC) - offset
        except ValueError:
            continue
    raise ValueError(f"unparseable Athena timestamp: {value!r}")


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() == "true"


def _parse_int(value: str | None) -> int | None:
    return None if value is None else int(value)


def _parse_float(value: str | None) -> float | None:
    """`staleness_seconds` can legitimately be infinite — `assess_source` returns
    `float("inf")` for a source that has never succeeded, and `RunHealth.to_dict` already
    renders that as null. Python's float() parses Athena's `Infinity` spelling."""
    return None if value is None else float(value)


def _coerce_article(row: dict[str, str | None]) -> dict[str, Any]:
    """One Athena row into the dict shape `dedup` and `ranker` already speak."""
    article_id = row["article_id"] or ""
    return {
        "article_id": article_id,
        "source_id": row["source_id"] or "",
        "url_canonical": row["url_canonical"] or "",
        # Text columns are nullable, and `group_stories` interpolates them into an
        # f-string. A None would tokenize as the literal word "none" and quietly become a
        # shared content word across every affected article.
        "title": row["title"] or "",
        "body_text": row["body_text"] or "",
        "published_at": _parse_timestamp(row["published_at"]),
        "fetched_at": _parse_timestamp(row["fetched_at"]),
        "publisher_domain": row["publisher_domain"] or "",
        # Stored signed: `normalize._to_signed_i64` reinterprets the unsigned simhash as
        # two's complement because pyarrow's safe cast raises above 2^63-1. int() handles
        # the leading minus, and `hashing.hamming` XOR-and-masks, so the round trip is
        # lossless and comparisons are unaffected. `tests/test_brief_read.py` pins this.
        "simhash": int(row["simhash"]) if row["simhash"] is not None else 0,
        # `exact_dedup` keys on this. A null would collapse every affected article into a
        # single "duplicate" and delete real stories from the brief, so fall back to the
        # article_id, which is unique by construction.
        "content_hash": row["content_hash"] or article_id,
        "timestamp_flagged": bool(_parse_bool(row["timestamp_flagged"])),
        "story_key": row["story_key"],
    }


def read_articles(
    since: datetime,
    until: datetime,
    *,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[list[dict[str, Any]], QueryResult]:
    """`silver.articles` over `[since, until)`, ready for `dedup`.

    `parse_error IS NULL` is not optional. SPEC §9 keeps a single bad entry inside an
    otherwise good feed in `articles` with its own `parse_error` rather than quarantining
    the whole row to `parse_rejects`, so those rows are present here and must not reach
    the ranker with an empty title.
    """
    sql = f"""
        SELECT {", ".join(_ARTICLE_COLUMNS)}
        FROM silver.articles
        WHERE event_date >= timestamp '{_sql_timestamp(since)}'
          AND event_date < timestamp '{_sql_timestamp(until)}'
          AND parse_error IS NULL
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )
    return [_coerce_article(row) for row in result.rows], result


def _coerce_health(row: dict[str, str | None]) -> SourceHealth:
    return SourceHealth(
        source_id=row["source_id"] or "",
        docs_ingested=_parse_int(row["docs_ingested"]) or 0,
        expected_min=_parse_int(row["expected_min"]) or 0,
        last_success_at=_parse_timestamp(row["last_success_at"]),
        staleness_seconds=_parse_float(row["staleness_seconds"]) or 0.0,
        status=row["status"] or "unmonitored",
        gap_reason=row["gap_reason"],
        content_staleness_seconds=_parse_float(row["content_staleness_seconds"]),
        baseline_docs=_parse_float(row["baseline_docs"]),
    )


def read_health(
    since: datetime,
    *,
    source_ids: tuple[str, ...] = DEPLOYED_SOURCE_IDS,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[list[SourceHealth], QueryResult]:
    """The newest health verdict per source, from `ops.source_health`.

    The footer reports what monitoring actually decided rather than re-deriving it here.
    Two implementations of "is this source healthy" would eventually disagree, and the one
    in the brief is the one nobody would think to test.

    A source with no verdict in the window is reported as `unmonitored` rather than
    omitted. Dropping it would render a missing source as a clean footer, which is the
    exact failure mode §11 exists to prevent — and the one 1.E found in `thin`.
    """
    sql = f"""
        SELECT {", ".join(_HEALTH_COLUMNS)}
        FROM ops.source_health
        WHERE window_start >= timestamp '{_sql_timestamp(since)}'
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    newest: dict[str, tuple[datetime, SourceHealth]] = {}
    for row in result.rows:
        window_start = _parse_timestamp(row["window_start"])
        if window_start is None:
            continue
        health = _coerce_health(row)
        seen = newest.get(health.source_id)
        if seen is None or window_start > seen[0]:
            newest[health.source_id] = (window_start, health)

    healths = [
        newest[source_id][1]
        if source_id in newest
        else SourceHealth(
            source_id=source_id,
            docs_ingested=0,
            expected_min=0,
            last_success_at=None,
            staleness_seconds=float("inf"),
            status="unmonitored",
        )
        for source_id in source_ids
    ]
    return healths, result
