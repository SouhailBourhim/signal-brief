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
from dataclasses import dataclass, field
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

# What the ranker and the template read off a cluster. `body_text` is deliberately absent:
# `silver.story_clusters` stores the *decision*, `silver.articles` stores the *content*, and
# duplicating a few hundred KB of prose into the cluster table to save one join would make
# the two disagree the first time an article was re-normalized.
_CLUSTER_COLUMNS = (
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

# What a mark is worth to the ranker. Symmetric on purpose: a thumbs-down that only removed
# a story would make the brief agree with itself over time, and §7.4 wants the marks to be
# instrumentation rather than a filter that narrows what it is possible to see.
_FEEDBACK_SCORES = {"up": 1.0, "down": -1.0}


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


# Trino renders `array<string>` into a result set as `[a, b, c]` — brackets, comma-space
# separated, no quoting. There is no escaping in that rendering, so a publisher domain
# containing a comma would split wrongly; domains cannot, which is why this is safe here and
# would not be for arbitrary text.
_ARRAY = re.compile(r"^\[(.*)\]$", re.DOTALL)


def _parse_array(value: str | None) -> list[str]:
    if not value:
        return []
    match = _ARRAY.match(value.strip())
    inner = match.group(1) if match else value
    return [item.strip() for item in inner.split(",") if item.strip()]


def _coerce_cluster(row: dict[str, str | None]) -> dict[str, Any]:
    """One `silver.story_clusters` row into the dict shape `ranker` already speaks.

    Identical keys to what `dedup.group_edges` produces in-process, so `score_cluster` cannot
    tell the two paths apart — which is the property that makes 3.0's in-process brief and
    3.D's table-backed one comparable at all.
    """
    return {
        "cluster_id": row["cluster_id"] or "",
        "canonical_article_id": row["canonical_article_id"] or "",
        "title": row["title"] or "",
        "url_canonical": row["url_canonical"] or "",
        "publisher_domain": row["publisher_domain"] or "",
        "published_at": _parse_timestamp(row["published_at"]),
        "fetched_at": _parse_timestamp(row["fetched_at"]),
        "first_seen": _parse_timestamp(row["first_seen"]),
        "last_seen": _parse_timestamp(row["last_seen"]),
        "article_count": _parse_int(row["article_count"]) or 1,
        "distinct_publisher_count": _parse_int(row["distinct_publisher_count"]) or 1,
        "publishers": _parse_array(row["publishers"]),
        "timestamp_flagged": bool(_parse_bool(row["timestamp_flagged"])),
        # Filled by `read_clusters` from the join, not stored on the cluster.
        "body_text": "",
        # Carried so the footer can say which algorithm produced what is on the page. A
        # brief rendered from clusters built by a version nobody remembers is not
        # reproducible, whatever the ordering key says.
        "algo_version": row["algo_version"],
        "ordering_key": row["ordering_key"],
        "entities": [],
    }


@dataclass(frozen=True)
class ClusterRead:
    """The newest clustered window, and enough about it to tell stale from empty.

    Both of those render as "no stories" and they are completely different faults: an empty
    window means ingestion stopped, a stale one means the cluster job did. SPEC §11's whole
    argument is that silence is the failure mode, so the brief has to be able to say which.
    """

    clusters: list[dict[str, Any]] = field(default_factory=list)
    window_start: datetime | None = None
    window_end: datetime | None = None
    algo_version: str | None = None

    @property
    def articles_in(self) -> int:
        """Articles that reached a cluster. Post-exact-dedup by construction, which is the
        same denominator `cluster_window` used for `dedup_ratio`."""
        return sum(c["article_count"] for c in self.clusters)


def read_clusters(
    since: datetime,
    until: datetime,
    *,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[ClusterRead, QueryResult]:
    """The most recent window in `silver.story_clusters`, with each head's body text.

    **The newest window, not a time range.** Consecutive daily runs share 48 of their 72
    hours, so one article appears in three windows under three possible `cluster_id`s
    (`spark/jobs/cluster.py`). Reading a range would show the same story three times; the
    brief wants the newest run's answer and nothing else.

    The join to `silver.articles` is what supplies the snippet under each headline. It is
    bounded by the same `event_date` predicate as the cluster window so the scan is
    partition-pruned — without it this reads the whole articles table to fetch a few hundred
    rows, which is SPEC §10.1's line item arriving by the back door.
    """
    columns = ", ".join(f"c.{name}" for name in _CLUSTER_COLUMNS)
    sql = f"""
        SELECT {columns}, a.body_text
        FROM silver.story_clusters c
        LEFT JOIN silver.articles a
          ON a.article_id = c.canonical_article_id
         AND a.event_date >= timestamp '{_sql_timestamp(since)}'
         AND a.event_date < timestamp '{_sql_timestamp(until)}'
        WHERE c.window_start = (SELECT max(window_start) FROM silver.story_clusters)
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    clusters = []
    for row in result.rows:
        cluster = _coerce_cluster(row)
        cluster["body_text"] = row.get("body_text") or ""
        clusters.append(cluster)

    first = result.rows[0] if result.rows else {}
    return (
        ClusterRead(
            clusters=clusters,
            window_start=_parse_timestamp(first.get("window_start")),
            window_end=_parse_timestamp(first.get("window_end")),
            algo_version=first.get("algo_version"),
        ),
        result,
    )


def read_cluster_entities(
    since: datetime,
    until: datetime,
    *,
    min_mentions: int = 1,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], QueryResult]:
    """`cluster_id -> the companies its articles mention`, most-mentioned first.

    Three tables, each doing the one job it exists for: `article_clusters` says which
    articles are in a cluster, `entity_mentions` says which companies those articles name,
    and `dim_entities` says what those ids mean **as of now** (`is_current`).

    Unlinked mentions are excluded here rather than filtered later. They are the majority of
    the table and the correct answer for most spans (SPEC §7.2), but a brief has nothing to
    show for "this article mentioned something that is not a company".
    """
    sql = f"""
        SELECT ac.cluster_id AS cluster_id,
               m.entity_id AS entity_id,
               arbitrary(e.canonical_name) AS canonical_name,
               arbitrary(e.ticker) AS ticker,
               count(*) AS mentions
        FROM silver.article_clusters ac
        JOIN silver.entity_mentions m
          ON m.article_id = ac.article_id
         AND m.event_date >= timestamp '{_sql_timestamp(since)}'
         AND m.event_date < timestamp '{_sql_timestamp(until)}'
        LEFT JOIN silver.dim_entities e
          ON e.entity_id = m.entity_id
         AND e.is_current
        WHERE ac.window_start = (SELECT max(window_start) FROM silver.article_clusters)
          AND m.entity_id IS NOT NULL
        GROUP BY ac.cluster_id, m.entity_id
        HAVING count(*) >= {min_mentions}
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    by_cluster: dict[str, list[dict[str, Any]]] = {}
    for row in result.rows:
        cluster_id = row["cluster_id"] or ""
        entity_id = row["entity_id"] or ""
        by_cluster.setdefault(cluster_id, []).append(
            {
                "entity_id": entity_id,
                # An id with no dimension row is a resolver/loader skew, not a reason to drop
                # the mention — falling back to the id keeps it visible in the brief, where
                # it is a lot more likely to be noticed than in a table.
                "canonical_name": row.get("canonical_name") or entity_id,
                "ticker": row.get("ticker"),
                "mentions": _parse_int(row.get("mentions")) or 1,
            }
        )
    for entities in by_cluster.values():
        entities.sort(key=lambda e: (-e["mentions"], e["entity_id"]))
    return by_cluster, result


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


# --------------------------------------------------------------------------------------
# Phase 4A: the three reads SPEC §7.4's remaining ranker components need.
# --------------------------------------------------------------------------------------


def read_hn_velocity(
    since: datetime,
    *,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[dict[str, float], QueryResult]:
    """`cluster_id -> points per hour`, from `silver.hn_score_snapshots`.

    Keyed on the cluster rather than the item, with the join done in Athena — the same
    choice `read_cluster_entities` makes, and for the same reason: the alternative ships
    every snapshot across the wire to do a dictionary lookup here (SPEC §10.1).

    The chain is `article_clusters` -> `articles.external_id` -> `hn_score_snapshots.item_id`.
    That middle hop is why 4A.H added `external_id` to `silver.articles`: `article_id` is
    derived from content, so it changes when a headline is edited — exactly when a story is
    still developing and its velocity matters most.

    The slope is between the oldest and newest snapshot in the window, not a fitted line.
    Two points is what the data reliably has for a story that entered the ranking recently,
    and a regression over three would claim a precision the sampling does not support.

    Stories with one snapshot are absent rather than zero: a single observation is not a
    slope, and zero would assert "not moving" about a story nobody has looked at twice.
    A cluster takes its fastest member — a cluster is one story, and if any copy of it is
    climbing, the story is climbing.
    """
    sql = f"""
        WITH slopes AS (
            SELECT item_id,
                   (max_by(score, observed_at) - min_by(score, observed_at))
                       / (date_diff('second', min(observed_at), max(observed_at)) / 3600.0)
                       AS points_per_hour
            FROM silver.hn_score_snapshots
            WHERE observed_at >= timestamp '{_sql_timestamp(since)}'
            GROUP BY item_id
            HAVING count(*) > 1
               AND date_diff('second', min(observed_at), max(observed_at)) > 0
        )
        SELECT ac.cluster_id AS cluster_id,
               max(s.points_per_hour) AS points_per_hour
        FROM silver.article_clusters ac
        JOIN silver.articles a
          ON a.article_id = ac.article_id
         AND a.source_id = 'hackernews'
         AND a.external_id IS NOT NULL
        JOIN slopes s
          ON s.item_id = a.external_id
        WHERE ac.window_start = (SELECT max(window_start) FROM silver.article_clusters)
        GROUP BY ac.cluster_id
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    slopes: dict[str, float] = {}
    for row in result.rows:
        cluster_id = row.get("cluster_id") or ""
        slope = _parse_float(row.get("points_per_hour"))
        if cluster_id and slope is not None:
            slopes[cluster_id] = slope
    return slopes, result


def read_market_moves(
    *,
    trailing_days: int = 20,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[dict[str, float], QueryResult]:
    """`ticker -> |latest return| / trailing stddev of returns`, from
    `silver.market_observations`.

    A z-like ratio rather than a boolean, so the ranker can scale rather than step and the
    number survives into `score_components` as something a reader can check (SPEC §7.4's
    explainability requirement). ADR-0010 records the threshold this feeds.

    Everything is computed in Athena rather than pulled and looped over here, for the same
    reason `read_cluster_entities` aggregates server-side: the alternative ships ~63 rows
    per ticker across the wire to compute one number each (SPEC §10.1).

    A ticker whose trailing standard deviation is zero — a halted stock, or a window with
    one bar — is absent rather than infinite.
    """
    sql = f"""
        WITH returns AS (
            SELECT ticker,
                   trade_date,
                   close / nullif(lag(close) OVER (
                       PARTITION BY ticker ORDER BY trade_date
                   ), 0) - 1 AS daily_return
            FROM silver.market_observations
        ),
        recent AS (
            SELECT ticker,
                   daily_return,
                   row_number() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS recency
            FROM returns
            WHERE daily_return IS NOT NULL
        )
        SELECT ticker,
               max_by(abs(daily_return), -recency) AS latest_move,
               stddev_samp(daily_return) AS trailing_stddev,
               count(*) AS observations
        FROM recent
        WHERE recency <= {trailing_days}
        GROUP BY ticker
        HAVING count(*) >= 3
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    moves: dict[str, float] = {}
    for row in result.rows:
        ticker = (row.get("ticker") or "").upper()
        latest = _parse_float(row.get("latest_move"))
        stddev = _parse_float(row.get("trailing_stddev"))
        if not ticker or latest is None or not stddev:
            continue
        moves[ticker] = latest / stddev
    return moves, result


def read_feedback(
    since: datetime,
    *,
    database: str | None = None,
    workgroup: str | None = None,
    client: Any | None = None,
) -> tuple[dict[str, float], QueryResult]:
    """`cluster_id -> +1.0 / -1.0`, the reader's own marks from `gold.brief_items`.

    SPEC §7.4: "your morning thumbs up/down", and §14 keeps automated weight-fitting behind
    "several hundred marked items" — so this is instrumentation feeding one hand-set weight,
    not a training signal.

    Keyed on `cluster_id`, which means a mark only carries forward while a story keeps the
    same cluster. Consecutive daily runs share 48 of 72 hours and `cluster.py` can assign a
    new id, so a mark's reach is a day or two by construction. That is the right lifetime
    for "I have seen this one" and the wrong one for training, which is another reason §14's
    bar sits where it does.
    """
    sql = f"""
        SELECT cluster_id, user_feedback
        FROM gold.brief_items
        WHERE brief_date >= date '{since.date().isoformat()}'
          AND user_feedback IS NOT NULL
    """
    result = run_query(
        sql,
        database=database or settings.athena_database,
        workgroup=workgroup or settings.athena_workgroup,
        client=client,
    )

    marks: dict[str, float] = {}
    for row in result.rows:
        cluster_id = row.get("cluster_id") or ""
        mark = (row.get("user_feedback") or "").strip().lower()
        if not cluster_id or mark not in _FEEDBACK_SCORES:
            continue
        marks[cluster_id] = _FEEDBACK_SCORES[mark]
    return marks, result
