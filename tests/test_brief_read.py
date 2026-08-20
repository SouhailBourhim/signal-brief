"""`brief/read.py` and `brief/build.py` — the 3.0 rung of SPEC §12's brief ladder.

Everything Athena hands back is a string or None, so this module is mostly about the
coercion boundary. The fake client routes on the SQL rather than replaying a fixed column
set, because `build.run` issues two different queries and the interesting bugs live in
telling them apart — `tests/test_athena.py`'s fake is shaped for state sequences instead.

No network, no AWS, no JVM.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from signal_core.brief.build import run
from signal_core.brief.read import (
    _coerce_article,
    _parse_timestamp,
    read_articles,
    read_health,
)
from signal_core.config import Settings
from signal_core.dedup import exact_dedup
from signal_core.hashing import hamming, simhash64
from signal_core.ops.health import DEGRADED_STATUSES, RunHealth
from signal_core.spark.jobs.normalize import _to_signed_i64

ARTICLE_COLUMNS = [
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
]

HEALTH_COLUMNS = [
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
]

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


def _article_row(**overrides: Any) -> list[str | None]:
    row: dict[str, str | None] = {
        "article_id": "a1",
        "source_id": "rss_tech",
        "url_canonical": "https://techcrunch.com/x",
        "title": "Northwind acquires Lumen Robotics",
        "body_text": "Northwind said on Tuesday it would acquire Lumen Robotics.",
        "published_at": "2026-08-20 10:00:00.000",
        "fetched_at": "2026-08-20 10:05:00.000",
        "publisher_domain": "techcrunch.com",
        "simhash": "123456789",
        "content_hash": "hash-a1",
        "timestamp_flagged": "false",
        "story_key": None,
    }
    row.update(overrides)
    return [row[c] for c in ARTICLE_COLUMNS]


def _health_row(**overrides: Any) -> list[str | None]:
    row: dict[str, str | None] = {
        "source_id": "rss_tech",
        "window_start": "2026-08-20 11:00:00.000",
        "docs_ingested": "12",
        "expected_min": "1",
        "last_success_at": "2026-08-20 11:30:00.000",
        "staleness_seconds": "1800.0",
        "status": "ok",
        "gap_reason": None,
        "content_staleness_seconds": "3600.0",
        "baseline_docs": "11.5",
    }
    row.update(overrides)
    return [row[c] for c in HEALTH_COLUMNS]


def _coerced(**overrides: Any) -> dict[str, Any]:
    """One Athena row, already coerced. `strict=True` so a column-list drift in this file
    fails here rather than silently shifting every value one to the left."""
    return _coerce_article(dict(zip(ARTICLE_COLUMNS, _article_row(**overrides), strict=True)))


class _Paginator:
    def __init__(self, columns: list[str], rows: list[list[str | None]]) -> None:
        self._columns, self._rows = columns, rows

    def paginate(self, **_: Any):
        yield {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": c} for c in self._columns]},
                    *(
                        {"Data": [({} if v is None else {"VarCharValue": v}) for v in row]}
                        for row in self._rows
                    ),
                ]
            }
        }


class _RoutingAthenaClient:
    """Answers `silver.articles` and `ops.source_health` with different column sets,
    and records every SQL it was asked so the tests can assert on the query itself."""

    def __init__(
        self,
        *,
        articles: list[list[str | None]] | None = None,
        healths: list[list[str | None]] | None = None,
        bytes_scanned: int = 4 * 1024 * 1024,
    ) -> None:
        self.articles = articles or []
        self.healths = healths or []
        self.bytes_scanned = bytes_scanned
        self.queries: list[str] = []
        self._current = ""

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self._current = kwargs["QueryString"]
        self.queries.append(self._current)
        return {"QueryExecutionId": "x"}

    def get_query_execution(self, QueryExecutionId: str) -> dict[str, Any]:
        del QueryExecutionId
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": self.bytes_scanned,
                    "EngineExecutionTimeInMillis": 120,
                },
            }
        }

    def get_paginator(self, operation_name: str) -> _Paginator:
        assert operation_name == "get_query_results"
        if "ops.source_health" in self._current:
            return _Paginator(HEALTH_COLUMNS, self.healths)
        return _Paginator(ARTICLE_COLUMNS, self.articles)


# --- coercion: the boundary where Athena's strings become typed rows ---------------------


def test_signed_simhash_round_trips_through_athena_as_a_string():
    """The one with teeth. `normalize._to_signed_i64` reinterprets an unsigned simhash as
    two's complement because pyarrow's safe cast raises above 2^63-1, so Athena hands back
    a negative decimal string for half of all articles. `hamming` XOR-and-masks, so the
    reinterpretation must not change any distance."""
    a_text, b_text = "Northwind acquires Lumen", "Northwind acquires Lumen Robotics"
    a_unsigned, b_unsigned = simhash64(a_text), simhash64(b_text)
    a_stored, b_stored = _to_signed_i64(a_unsigned), _to_signed_i64(b_unsigned)

    a = _coerced(simhash=str(a_stored))
    b = _coerced(simhash=str(b_stored))

    assert hamming(a["simhash"], b["simhash"]) == hamming(a_unsigned, b_unsigned)


def test_a_simhash_above_two_to_the_sixty_three_survives_the_trip():
    big = (1 << 64) - 3  # unsigned, well past what a signed long can hold
    stored = _to_signed_i64(big)
    assert stored < 0
    coerced = _coerced(simhash=str(stored))
    assert hamming(coerced["simhash"], big) == 0


def test_null_text_columns_become_empty_strings_not_the_word_none():
    """`group_stories` interpolates title and body into an f-string. A None would tokenize
    as the literal word "none" and become a content word shared by every affected
    article — a silent path to false merges."""
    coerced = _coerced(title=None, body_text=None)
    assert coerced["title"] == ""
    assert coerced["body_text"] == ""


def test_null_content_hash_falls_back_to_article_id():
    """`exact_dedup` keys on content_hash. Sharing a None would collapse unrelated
    articles into one and delete real stories from the brief."""
    rows = [_coerced(article_id=a, content_hash=None) for a in ("a1", "a2")]
    kept, removed = exact_dedup(rows)
    assert removed == 0
    assert len(kept) == 2


def test_null_published_at_stays_none_and_flagged_parses():
    coerced = _coerced(published_at=None, timestamp_flagged="true")
    assert coerced["published_at"] is None
    assert coerced["timestamp_flagged"] is True


@pytest.mark.parametrize(
    "rendered",
    [
        # What Athena's Iceberg connector actually returns. 3.0 found this on its first
        # real row, having been written against the middle one.
        "2026-08-20 10:00:00.000000 UTC",
        "2026-08-20 10:00:00.000",
        "2026-08-20 10:00:00",
        "2026-08-20T10:00:00Z",
        "2026-08-20T10:00:00.000000 UTC",
    ],
)
def test_every_shape_athena_renders_parses_to_the_same_instant(rendered):
    assert _parse_timestamp(rendered) == datetime(2026, 8, 20, 10, tzinfo=UTC)


def test_none_stays_none():
    assert _parse_timestamp(None) is None


@pytest.mark.parametrize(
    ("rendered", "expected_hour"),
    [("2026-08-20 12:00:00.000 +02:00", 10), ("2026-08-20 08:00:00 -02:00", 10)],
)
def test_an_offset_is_converted_not_discarded(rendered, expected_hour):
    """Trimming a `+02:00` instead of reading it would shift every timestamp in the brief
    by two hours — the silent-shift failure `timeutil.ensure_utc` refuses to commit."""
    assert _parse_timestamp(rendered) == datetime(2026, 8, 20, expected_hour, tzinfo=UTC)


def test_a_zone_name_is_not_mangled_by_the_iso_separator_fix():
    """`.replace("T", " ")` also eats the T in `UTC`, leaving `U C` and an unmatchable
    zone. That is the bug that broke the first real run; this is its regression test."""
    assert _parse_timestamp("2026-08-20 10:00:00.000000 UTC") is not None


def test_an_unparseable_timestamp_raises_rather_than_reading_as_null():
    """Returning None would render a schema change as a null column, which the footer
    would then report as missing data rather than a bug."""
    with pytest.raises(ValueError, match="unparseable"):
        _parse_timestamp("last tuesday")


# --- the queries themselves --------------------------------------------------------------


def test_article_query_prunes_partitions_projects_columns_and_drops_parse_errors():
    client = _RoutingAthenaClient(articles=[_article_row()])
    read_articles(NOW - timedelta(hours=72), NOW, client=client)
    sql = client.queries[0]

    assert "event_date >= timestamp '2026-08-17 12:00:00'" in sql
    assert "event_date < timestamp '2026-08-20 12:00:00'" in sql
    assert "parse_error IS NULL" in sql
    assert "SELECT *" not in sql
    assert "payload" not in sql  # projection is the larger half of the bytes-scanned win


def test_reported_bytes_and_cost_come_from_athena_not_from_us():
    client = _RoutingAthenaClient(articles=[_article_row()], bytes_scanned=7 * 1024 * 1024)
    _, result = read_articles(NOW - timedelta(hours=72), NOW, client=client)
    assert result.bytes_scanned == 7 * 1024 * 1024
    assert result.cost_usd > 0


# --- health: what the footer reports -----------------------------------------------------


def test_newest_verdict_per_source_wins():
    client = _RoutingAthenaClient(
        healths=[
            _health_row(window_start="2026-08-20 09:00:00.000", status="ok"),
            _health_row(window_start="2026-08-20 11:00:00.000", status="dead_feed"),
            _health_row(window_start="2026-08-20 10:00:00.000", status="thin"),
        ]
    )
    healths, _ = read_health(NOW - timedelta(hours=168), source_ids=("rss_tech",), client=client)
    assert [h.status for h in healths] == ["dead_feed"]


def test_a_source_with_no_verdict_is_reported_unmonitored_not_dropped():
    """SPEC §11's whole point: silence must not render as health. 1.E found the same bug
    in `thin`, where a status existed that nothing acted on."""
    client = _RoutingAthenaClient(healths=[_health_row(source_id="rss_tech")])
    healths, _ = read_health(
        NOW - timedelta(hours=168), source_ids=("rss_tech", "edgar"), client=client
    )

    assert [h.source_id for h in healths] == ["rss_tech", "edgar"]
    assert healths[1].status == "unmonitored"
    assert "unmonitored" in DEGRADED_STATUSES
    assert RunHealth(sources=healths).status == "degraded"


def test_gap_reason_survives_to_the_footer():
    client = _RoutingAthenaClient(
        healths=[_health_row(status="gapped", gap_reason="rss_tech keeps only the current feed")]
    )
    healths, _ = read_health(NOW - timedelta(hours=168), source_ids=("rss_tech",), client=client)
    assert healths[0].gap_reason == "rss_tech keeps only the current feed"


# --- end to end --------------------------------------------------------------------------


def test_run_writes_a_brief_whose_footer_reports_what_the_queries_cost(tmp_path):
    client = _RoutingAthenaClient(
        articles=[
            _article_row(article_id="a1", content_hash="h1"),
            _article_row(article_id="a2", content_hash="h2", publisher_domain="theverge.com"),
            _article_row(article_id="a3", content_hash="h1"),  # exact duplicate of a1
        ],
        healths=[_health_row()],
        bytes_scanned=3 * 1024 * 1024,
    )
    path = run(
        Settings(out_root=tmp_path),
        limit=5,
        date="2026-08-20",
        now=NOW,
        client=client,
    )

    assert path == tmp_path / "brief-2026-08-20.html"
    html = path.read_text(encoding="utf-8")
    # Both queries are summed into the footer: 2 x 3 MiB scanned, and a cost that is two
    # 10 MB-floored scans rather than one (SPEC §17 — never report a number the bill won't
    # match).
    assert "6,291,456 bytes scanned" in html
    assert "$0.0001" in html
    assert "Northwind" in html
    # a3 is byte-identical to a1 and must not appear as a second story.
    assert html.count("Northwind acquires Lumen Robotics") <= 2
