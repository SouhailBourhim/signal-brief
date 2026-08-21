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
from signal_core.brief.ranker import score_cluster
from signal_core.brief.read import (
    _coerce_article,
    _parse_array,
    _parse_timestamp,
    read_articles,
    read_cluster_entities,
    read_clusters,
    read_health,
)
from signal_core.brief.render import SNIPPET_CHARS, render_brief, snippet
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

CLUSTER_COLUMNS = [
    "cluster_id",
    "canonical_article_id",
    "title",
    "url_canonical",
    "publisher_domain",
    "published_at",
    "fetched_at",
    "first_seen",
    "last_seen",
    "article_count",
    "distinct_publisher_count",
    "publishers",
    "timestamp_flagged",
    "algo_version",
    "ordering_key",
    "window_start",
    "window_end",
    # Joined from `silver.articles`, not stored on the cluster.
    "body_text",
]

ENTITY_COLUMNS = ["cluster_id", "entity_id", "canonical_name", "ticker", "mentions"]

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


def _cluster_row(**overrides: Any) -> list[str | None]:
    row: dict[str, str | None] = {
        "cluster_id": "c1",
        "canonical_article_id": "a1",
        "title": "Northwind acquires Lumen Robotics",
        "url_canonical": "https://techcrunch.com/x",
        "publisher_domain": "techcrunch.com",
        "published_at": "2026-08-20 10:00:00.000000 UTC",
        "fetched_at": "2026-08-20 10:05:00.000000 UTC",
        "first_seen": "2026-08-20 09:00:00.000000 UTC",
        "last_seen": "2026-08-20 11:00:00.000000 UTC",
        "article_count": "3",
        "distinct_publisher_count": "2",
        "publishers": "[techcrunch.com, theverge.com]",
        "timestamp_flagged": "false",
        "algo_version": "3.B.4",
        "ordering_key": "fetched_at,article_id@abc123",
        "window_start": "2026-08-20 05:00:00.000000 UTC",
        "window_end": "2026-08-20 11:59:00.000000 UTC",
        "body_text": "Northwind said on Tuesday it would acquire Lumen Robotics.",
    }
    row.update(overrides)
    return [row[c] for c in CLUSTER_COLUMNS]


def _entity_row(**overrides: Any) -> list[str | None]:
    row: dict[str, str | None] = {
        "cluster_id": "c1",
        "entity_id": "NWND",
        "canonical_name": "Northwind Corp",
        "ticker": "NWND",
        "mentions": "4",
    }
    row.update(overrides)
    return [row[c] for c in ENTITY_COLUMNS]


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
    """Answers each of the brief's four queries with its own column set, and records every
    SQL it was asked so the tests can assert on the query itself.

    Routing order matters: the cluster query names `silver.articles` too (it joins to it for
    the snippet), so `silver.story_clusters` has to be checked first."""

    def __init__(
        self,
        *,
        articles: list[list[str | None]] | None = None,
        clusters: list[list[str | None]] | None = None,
        entities: list[list[str | None]] | None = None,
        healths: list[list[str | None]] | None = None,
        bytes_scanned: int = 4 * 1024 * 1024,
    ) -> None:
        self.articles = articles or []
        self.clusters = clusters or []
        self.entities = entities or []
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
        if "silver.story_clusters" in self._current:
            return _Paginator(CLUSTER_COLUMNS, self.clusters)
        if "silver.entity_mentions" in self._current:
            return _Paginator(ENTITY_COLUMNS, self.entities)
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


# --- clusters: 3.D reads the tables 3.B and 3.C write ------------------------------------


def test_cluster_query_reads_the_newest_window_not_a_time_range():
    """Consecutive daily runs share 48 of their 72 hours, so one article sits in three
    windows under three cluster ids. Reading a range would show each story three times."""
    client = _RoutingAthenaClient(clusters=[_cluster_row()])
    read_clusters(NOW - timedelta(hours=72), NOW, client=client)
    sql = client.queries[0]

    assert "max(window_start)" in sql
    assert "FROM silver.story_clusters" in sql


def test_the_snippet_join_is_partition_pruned():
    """Without the event_date bounds on the joined side this reads the whole articles table
    to fetch a few hundred body_text values — SPEC §10.1 arriving by the back door."""
    client = _RoutingAthenaClient(clusters=[_cluster_row()])
    read_clusters(NOW - timedelta(hours=72), NOW, client=client)
    sql = client.queries[0]

    assert "a.event_date >= timestamp '2026-08-17 12:00:00'" in sql
    assert "a.event_date < timestamp '2026-08-20 12:00:00'" in sql


def test_a_cluster_row_coerces_into_what_the_ranker_already_speaks():
    """The keys must match `dedup.group_edges`' output exactly, or `score_cluster` can tell
    the table-backed path from the in-process one — and only one of them is under test."""
    client = _RoutingAthenaClient(clusters=[_cluster_row()])
    read, _ = read_clusters(NOW - timedelta(hours=72), NOW, client=client)
    cluster = read.clusters[0]

    assert cluster["cluster_id"] == "c1"
    assert cluster["distinct_publisher_count"] == 2
    assert cluster["publishers"] == ["techcrunch.com", "theverge.com"]
    assert cluster["last_seen"] == datetime(2026, 8, 20, 11, tzinfo=UTC)
    assert cluster["body_text"].startswith("Northwind said")

    scored = score_cluster(cluster, now=NOW)
    assert set(scored["score_components"]) == {"breadth", "recency"}


def test_articles_in_is_the_denominator_the_cluster_job_used():
    """`dedup_ratio` in the footer has to mean the same thing it means in `cluster_window`,
    which counted articles that reached a cluster — post exact-dedup."""
    client = _RoutingAthenaClient(
        clusters=[
            _cluster_row(cluster_id="c1", article_count="3"),
            _cluster_row(cluster_id="c2", article_count="1"),
        ]
    )
    read, _ = read_clusters(NOW - timedelta(hours=72), NOW, client=client)
    assert read.articles_in == 4


def test_an_empty_cluster_table_is_distinguishable_from_a_stale_one():
    """Both render as "no stories" and they are opposite faults — empty means ingestion
    stopped, stale means the cluster job did."""
    client = _RoutingAthenaClient(clusters=[])
    read, _ = read_clusters(NOW - timedelta(hours=72), NOW, client=client)

    assert read.clusters == []
    assert read.window_start is None, "nothing to be stale about"


@pytest.mark.parametrize(
    ("rendered", "expected"),
    [
        ("[techcrunch.com, theverge.com]", ["techcrunch.com", "theverge.com"]),
        ("[techcrunch.com]", ["techcrunch.com"]),
        ("[]", []),
        (None, []),
    ],
)
def test_trino_renders_arrays_with_brackets_and_no_quoting(rendered, expected):
    assert _parse_array(rendered) == expected


# --- entities ----------------------------------------------------------------------------


def test_entity_query_joins_the_three_tables_and_drops_unlinked_mentions():
    """Unlinked is the correct answer for most spans (SPEC §7.2) and the majority of the
    table, but a brief has nothing to show for "this mentioned something that is not a
    company"."""
    client = _RoutingAthenaClient(entities=[_entity_row()])
    read_cluster_entities(NOW - timedelta(hours=72), NOW, client=client)
    sql = client.queries[0]

    assert "silver.article_clusters" in sql
    assert "silver.entity_mentions" in sql
    assert "silver.dim_entities" in sql
    assert "m.entity_id IS NOT NULL" in sql
    assert "e.is_current" in sql, "the dimension is SCD2; the brief wants today's names"


def test_entities_come_back_grouped_by_cluster_most_mentioned_first():
    client = _RoutingAthenaClient(
        entities=[
            _entity_row(
                cluster_id="c1",
                entity_id="AAPL",
                canonical_name="Apple Inc.",
                ticker="AAPL",
                mentions="1",
            ),
            _entity_row(cluster_id="c1", entity_id="NWND", mentions="4"),
            _entity_row(
                cluster_id="c2",
                entity_id="openai",
                canonical_name="OpenAI",
                ticker=None,
                mentions="2",
            ),
        ]
    )
    by_cluster, _ = read_cluster_entities(NOW - timedelta(hours=72), NOW, client=client)

    assert [e["entity_id"] for e in by_cluster["c1"]] == ["NWND", "AAPL"]
    assert by_cluster["c2"][0]["ticker"] is None, "a private company has no ticker to show"


def test_an_entity_with_no_dimension_row_keeps_its_id_rather_than_vanishing():
    """Resolver/loader skew is worth seeing in the brief, where it will be noticed."""
    client = _RoutingAthenaClient(
        entities=[_entity_row(entity_id="mystery-co", canonical_name=None, ticker=None)]
    )
    by_cluster, _ = read_cluster_entities(NOW - timedelta(hours=72), NOW, client=client)
    assert by_cluster["c1"][0]["canonical_name"] == "mystery-co"


# --- end to end --------------------------------------------------------------------------


def test_run_writes_a_brief_from_the_cluster_tables_with_costs_from_all_three_queries(tmp_path):
    client = _RoutingAthenaClient(
        clusters=[_cluster_row(cluster_id="c1"), _cluster_row(cluster_id="c2", title="Second")],
        entities=[_entity_row(cluster_id="c1")],
        healths=[_health_row()],
        bytes_scanned=3 * 1024 * 1024,
    )
    path = run(Settings(out_root=tmp_path), limit=5, date="2026-08-20", now=NOW, client=client)

    assert path == tmp_path / "brief-2026-08-20.html"
    html = path.read_text(encoding="utf-8")
    # Three queries now, not two: clusters, entities, health.
    assert "9,437,184 bytes scanned" in html
    assert "Northwind" in html
    # The resolved company shows on the story, with its ticker.
    assert "Northwind Corp" in html
    assert "2 articles in" not in html, "articles_in comes from article_count, not row count"
    assert "6 articles in" in html


def test_the_brief_does_not_report_an_exact_dupe_count_it_no_longer_measures(tmp_path):
    """Collapsing exact duplicates happens in `cluster_window` now. Printing a 0 here would
    be a number nobody measured, which SPEC §17 rules out."""
    client = _RoutingAthenaClient(clusters=[_cluster_row()], healths=[_health_row()])
    path = run(Settings(out_root=tmp_path), limit=5, date="2026-08-20", now=NOW, client=client)

    assert "exact dupes removed" not in path.read_text(encoding="utf-8")


def test_an_empty_cluster_table_still_renders_a_brief_with_an_honest_footer(tmp_path, capsys):
    """A missing cluster run must not look like a quiet news day."""
    client = _RoutingAthenaClient(clusters=[], healths=[_health_row()])
    path = run(Settings(out_root=tmp_path), limit=5, date="2026-08-20", now=NOW, client=client)

    assert path.exists()
    assert "WARNING: no clusters" in capsys.readouterr().out


def test_a_stale_clustered_window_says_so(tmp_path, capsys):
    """Yesterday's stories under today's date, rendered without comment, is exactly the
    silence SPEC §11 exists to prevent."""
    client = _RoutingAthenaClient(
        clusters=[_cluster_row(window_end="2026-08-18 05:00:00.000000 UTC")],
        healths=[_health_row()],
    )
    run(Settings(out_root=tmp_path), limit=5, date="2026-08-20", now=NOW, client=client)

    assert "WARNING: newest clustered window is" in capsys.readouterr().out


def test_a_fresh_window_does_not_warn(tmp_path, capsys):
    """`window_start` is 72 hours before the run by construction, so measuring staleness
    from it fired on every healthy brief. A warning that is always on is one nobody reads —
    found by running the real thing, not by a test."""
    client = _RoutingAthenaClient(clusters=[_cluster_row()], healths=[_health_row()])
    run(Settings(out_root=tmp_path), limit=5, date="2026-08-20", now=NOW, client=client)

    assert "WARNING: newest clustered window" not in capsys.readouterr().out


def test_snippet_shows_prose_not_the_markup_the_feed_sent():
    """The template autoescapes untrusted feed content, so raw markup renders as visible
    tags. Found by reading the brief: a Tesla headline sat above `<figure><img alt=...
    data-portal-copyright=...>` where the story should have been."""
    raw = (
        '<figure><img alt="Tesla Solar Roof" '
        'data-portal-copyright="Image: Dieter Bohn / &lt;em&gt;The Verge&lt;/em&gt;" '
        'src="https://platform.theverge.com/x.png?quality=90" /></figure>'
        '<p class="wp-block-paragraph">Tesla has discontinued Solar Roof.</p>'
    )
    assert snippet(raw) == "Tesla has discontinued Solar Roof."


def test_snippet_keeps_edgar_field_names_readable():
    """An EDGAR body is `<b>`-wrapped field labels. Stripping the markup must leave the
    fields, because for a filing they are the whole of the content."""
    body = "<b>Filed:</b> 2026-08-21 <b>AccNo:</b> 0001193125-26-360544 <b>Size:</b> 197 KB"
    assert snippet(body) == "Filed: 2026-08-21 AccNo: 0001193125-26-360544 Size: 197 KB"


def test_snippet_only_claims_there_is_more_when_there_is():
    short = "Two sentences. That is all of it."
    assert snippet(short) == short
    assert not snippet(short).endswith("…")

    long = "word " * 400
    cut = snippet(long)
    assert cut.endswith("…")
    assert len(cut) <= SNIPPET_CHARS + 2
    # Cut at a word boundary, never mid-word.
    assert not cut.removesuffix(" …").endswith("wor")


def test_snippet_is_safe_on_an_empty_body():
    assert snippet("") == ""


def test_every_render_path_gets_a_cleaned_snippet():
    """`render_brief` cleans, not the reader that built the clusters.

    The first cut of this did it in `read_clusters`, which is the Athena path only — and
    `skeleton.py` builds its clusters in process through `group_stories`, so `make skeleton`
    rendered a brief with no body text under any headline. Cleaning at the render boundary is
    what makes the two paths agree.
    """
    clusters = [
        {
            "title": "T",
            "publisher_domain": "x.com",
            "score": 0.5,
            "score_components": {},
            "distinct_publisher_count": 1,
            "publishers": ["x.com"],
            "body_text": "<p>Northwind acquires Lumen.</p>",
            "entities": [],
        }
    ]
    html_out = render_brief(clusters, RunHealth(articles_in=1, clusters_out=1), date="2026-08-21")
    assert "Northwind acquires Lumen." in html_out
    assert "<p>Northwind" not in html_out.replace('<p class="body">', "")
