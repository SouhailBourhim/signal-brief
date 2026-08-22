"""Bronze -> silver, on Spark. SPEC §7, §9; docs/runbooks/phase-2.md 2.C; ADR-0007.

All domain logic lives in `signal_core.parse` (bytes -> `ParsedItem`/`ParsedComment`) and
`signal_core.transform` (`ParsedItem` -> silver row), neither of which imports Spark and
both are unit-tested without a JVM; this module only handles distribution, schema, and
the sink.

**Two bronze paths, two normalize entry points** (CLAUDE.md: "don't conflate them"):

- `normalize()` reads the Phase 0 skeleton's local Parquet layout directly — no Iceberg,
  no Glue, no network — because `make skeleton` must run on a fresh clone with nothing
  but PyPI. `skeleton.py` is the only caller.
- `normalize_window()` and `normalize_hn_comments_window()` are the real Phase 2 job:
  read a window of `bronze.raw_documents` (Iceberg), MERGE into `silver.articles` /
  `silver.hn_comments` / `silver.parse_rejects`. Modeled on `commit_bronze.py`'s shape —
  `ensure_table` + windowed read + MERGE + a result dataclass — and reachable only
  through `build_iceberg_session`, never the skeleton.

**One bronze row can become N silver rows.** A real RSS/Atom row is a whole feed, not
one article, unlike the Phase 0 fake source this job was originally written against.
`mapInPandas` supports that directly: the callback yields one `pandas.DataFrame` per
input partition, and nothing requires that frame to have the same row count as the
partition it was built from.

**Two Spark passes for Hacker News, not one union schema.** Bronze is partitioned by
`source_id`, so the comments pass reads only the `hackernews` partitions — cheap — and
each job keeps exactly one output schema, rather than a discriminator column nothing
else needs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from signal_core.parse import get_parser
from signal_core.spark.jobs.commit_bronze import BRONZE_TABLE
from signal_core.timeutil import ensure_utc, utc_now
from signal_core.transform import to_article

if TYPE_CHECKING:  # keeps importing this module cheap for non-Spark callers
    from datetime import datetime

    from pyspark.sql import DataFrame, SparkSession

# --------------------------------------------------------------------------------------
# Shared: one bronze row -> zero or more silver rows. Used by both entry points below.
# --------------------------------------------------------------------------------------

# Order matters: mapInPandas matches the schema positionally. `ingest_id` rides along
# only as plumbing for `silver.parse_rejects` (see `_split_articles_and_rejects`) — it
# is dropped before anything is written to `silver.articles`, which stores exactly SPEC
# §9's columns plus the already-established `timestamp_flagged`/`story_key`/`parse_error`
# and ADR-0007's `event_date`.
SILVER_COLUMNS = [
    "article_id",
    "source_id",
    "url_canonical",
    "title",
    "body_text",
    "published_at",
    "fetched_at",
    "event_date",
    "lang",
    "publisher_domain",
    "authority_score",
    "simhash",
    "content_hash",
    "timestamp_flagged",
    "story_key",
    "parse_error",
    "ingest_id",
]

SILVER_SCHEMA = """
    article_id string, source_id string, url_canonical string, title string,
    body_text string, published_at timestamp, fetched_at timestamp,
    event_date timestamp, lang string, publisher_domain string, authority_score double,
    simhash long, content_hash string, timestamp_flagged boolean, story_key string,
    parse_error string, ingest_id string
"""


def _normalize_row(row: dict) -> list[dict]:
    """One bronze row -> zero or more silver rows.

    A row-level parse failure (bad format, undecodable payload even after
    `feedparse.py`'s encoding-lie fallback) still produces exactly one quarantined
    silver row, so a completely unparseable feed is as visible to `split_rejects` as a
    single bad entry within an otherwise good one — SPEC §6.2's "never silently
    dropped" applies to both. `ParseResult.comments` (Hacker News) is intentionally not
    emitted here; `normalize_hn_comments_window`'s second pass owns those.
    """
    result = get_parser(row["source_id"])(row["payload"])
    ingest_id = row.get("ingest_id", "")
    if result.error:
        fetched_at = row["fetched_at"]
        return [
            {
                "article_id": "",
                "source_id": row["source_id"],
                "url_canonical": "",
                "title": "",
                "body_text": "",
                "published_at": None,
                "fetched_at": fetched_at,
                "event_date": fetched_at,
                "lang": "en",
                "publisher_domain": "",
                "authority_score": 0.5,
                "simhash": 0,
                "content_hash": row.get("content_hash", ""),
                "timestamp_flagged": True,
                "story_key": None,
                "parse_error": result.error,
                "ingest_id": ingest_id,
            }
        ]
    articles = []
    for item in result.items:
        article = to_article(item, row)
        article["simhash"] = _to_signed_i64(article["simhash"])
        articles.append({**article, "ingest_id": ingest_id})
    return articles


def _to_signed_i64(value: int) -> int:
    """Reinterpret an unsigned 64-bit bit pattern as signed two's complement.

    `hashing.simhash64` returns an unsigned value up to `2**64 - 1`; Iceberg/Spark's
    `long` is signed 64-bit, and pyarrow's safe cast raises rather than silently
    wrapping a value above `2**63 - 1` (real article text hits this often enough that
    the fake fixture's dodging it was luck, not correctness — SILVER_SCHEMA's `simhash
    long` has been wrong since 2.B). Lossless: `dedup.hamming` XORs and masks to 64
    bits rather than comparing magnitudes, so the same bits read back as signed compare
    identically to the unsigned value `to_article` (and the skeleton's Spark-free path)
    still returns directly.
    """
    return value - (1 << 64) if value >= (1 << 63) else value


def _normalize_partitions(iterator):
    """mapInPandas body: one pandas frame in, one normalized frame out — not
    necessarily the same row count, since a single RSS/Atom bronze row parses into N
    articles."""
    import pandas as pd

    for pdf in iterator:
        rows = [row for record in pdf.to_dict("records") for row in _normalize_row(record)]
        yield pd.DataFrame(rows, columns=SILVER_COLUMNS)


def split_rejects(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(clean, quarantined) — SPEC §6.2."""
    return silver.filter("parse_error IS NULL"), silver.filter("parse_error IS NOT NULL")


# --------------------------------------------------------------------------------------
# Skeleton path: local Parquet, no Iceberg, no network. `skeleton.py` is the only caller.
# --------------------------------------------------------------------------------------


def normalize(spark: SparkSession, bronze_root: Path) -> DataFrame:
    """Read the Phase 0 local Parquet layout, normalize each row, return silver rows.

    Rows that fail to parse are kept with a populated `parse_error` rather than dropped
    — SPEC §6.2 requires quarantine with a reason. The caller splits them via
    `split_rejects`; this job never silently loses a record.
    """
    bronze = spark.read.parquet(str(bronze_root))
    return bronze.mapInPandas(_normalize_partitions, schema=SILVER_SCHEMA)


# --------------------------------------------------------------------------------------
# Production path: `bronze.raw_documents` (Iceberg) -> `silver.articles` /
# `silver.hn_comments` / `silver.parse_rejects`, MERGEd. Never reached by the skeleton.
# --------------------------------------------------------------------------------------

ARTICLES_TABLE = "silver.articles"
HN_COMMENTS_TABLE = "silver.hn_comments"
PARSE_REJECTS_TABLE = "silver.parse_rejects"
HN_SCORES_TABLE = "silver.hn_score_snapshots"

# Sources whose documents are not articles and never become any. Excluded from the articles
# pass rather than parsed and discarded: `_normalize_row` would return zero rows for each of
# them anyway, but they would still be counted in `NormalizeResult.bronze_rows`, which is
# reported. `hn_scores` alone commits ~240 documents an hour, so leaving it in would show a
# rising bronze count against a flat article count — the exact shape of a broken parser,
# permanently, in a metric SPEC §11 expects someone to read.
NON_ARTICLE_SOURCES: tuple[str, ...] = ("hn_scores",)

# Same properties as `commit_bronze.py`'s bronze table, for the same reason: payloads
# and article bodies are whole documents, not tiny rows, so small-file fragmentation is
# a real risk without a target file size from the start.
_TBLPROPERTIES = """
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes' = '134217728',
        'write.merge.isolation-level' = 'serializable'
    )
"""

# `MERGE ... WHEN NOT MATCHED THEN INSERT` is **not** a uniqueness constraint. It compiles to
# an append, and Iceberg appends never conflict with each other, so two writers that both
# read a pre-insert snapshot both find NOT MATCHED and both insert. `silver.articles` carries
# 132 duplicate `article_id` rows from exactly that — all of them fetched inside one hour on
# 2026-08-19, the session where `process` was first unpaused and manually triggered alongside
# its own schedule (docs/runbooks/phase-3.md 3.B).
#
# Serializable isolation makes the second writer fail loudly instead of duplicating quietly.
# Applied by ALTER as well as by CREATE, because `CREATE TABLE IF NOT EXISTS` is a no-op
# against a live table — the same reason `health_snapshot` has to ALTER its added columns.
_ADDED_PROPERTIES = (("write.merge.isolation-level", "serializable"),)

ARTICLES_DDL = """
    article_id string NOT NULL,
    source_id string NOT NULL,
    url_canonical string,
    title string,
    body_text string,
    published_at timestamp,
    fetched_at timestamp NOT NULL,
    event_date timestamp NOT NULL,
    lang string,
    publisher_domain string,
    authority_score double,
    simhash long,
    content_hash string,
    timestamp_flagged boolean,
    story_key string,
    parse_error string
"""

# Matches `ARTICLES_DDL`'s column order exactly — used to strip `SILVER_COLUMNS`' extra
# `ingest_id` plumbing column and guarantee `MERGE ... INSERT *` lines up positionally.
_ARTICLES_COLUMNS = [
    "article_id",
    "source_id",
    "url_canonical",
    "title",
    "body_text",
    "published_at",
    "fetched_at",
    "event_date",
    "lang",
    "publisher_domain",
    "authority_score",
    "simhash",
    "content_hash",
    "timestamp_flagged",
    "story_key",
    "parse_error",
]

# `story_id` is resolved by `_resolve_story_ids` below, walking `parent_id` up the
# comment graph — a story never appears as a comment, so the id where the chain stops
# being a known comment *is* the story. `created_at` is nullable: HN's `time` field is
# defensively optional even though real comments always carry it.
HN_COMMENTS_DDL = """
    item_id string NOT NULL,
    parent_id string,
    story_id string,
    by string,
    text string,
    created_at timestamp,
    fetched_at timestamp NOT NULL,
    ingest_id string NOT NULL,
    dead boolean,
    deleted boolean
"""

# One row per (story, observation). SPEC §7.4's velocity component needs a slope, and a
# slope needs the same id measured more than once — which is the whole reason `hn_scores`
# exists as a separate source (see sources/hn_scores.py).
#
# `observed_at` is the bronze row's `fetched_at`, not anything in the payload. HN's `time`
# field is the *submission* time and never moves, so keying on it would give every snapshot
# of a story an identical timestamp and a slope over a zero interval. The parser cannot see
# `fetched_at`, so this pass supplies it — the one field here that comes from the envelope
# rather than the document.
HN_SCORES_DDL = """
    item_id string NOT NULL,
    score int NOT NULL,
    descendants int,
    title string,
    observed_at timestamp NOT NULL,
    ingest_id string NOT NULL
"""

# No payload column: bronze already has it, and copying it here would double storage of
# the thing most likely to be large, for a table that exists to hold a reason string.
PARSE_REJECTS_DDL = """
    ingest_id string NOT NULL,
    source_id string NOT NULL,
    parse_error string,
    fetched_at timestamp,
    rejected_at timestamp NOT NULL
"""


def ensure_tables(
    spark: SparkSession,
    *,
    articles_table: str = ARTICLES_TABLE,
    comments_table: str = HN_COMMENTS_TABLE,
    rejects_table: str = PARSE_REJECTS_TABLE,
    scores_table: str = HN_SCORES_TABLE,
) -> None:
    """Create `silver`'s four tables if they aren't there yet."""
    namespace = articles_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {articles_table} ({ARTICLES_DDL}) "
        f"USING iceberg PARTITIONED BY (days(event_date)) {_TBLPROPERTIES}"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {comments_table} ({HN_COMMENTS_DDL}) "
        f"USING iceberg PARTITIONED BY (days(created_at)) {_TBLPROPERTIES}"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {rejects_table} ({PARSE_REJECTS_DDL}) "
        f"USING iceberg {_TBLPROPERTIES}"
    )
    # Partitioned on observation time, not submission time: this table is read as "the last
    # N hours of snapshots" by the ranker, and a story submitted last week still produces
    # rows today.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {scores_table} ({HN_SCORES_DDL}) "
        f"USING iceberg PARTITIONED BY (days(observed_at)) {_TBLPROPERTIES}"
    )
    for table in (articles_table, comments_table, rejects_table, scores_table):
        for name, value in _ADDED_PROPERTIES:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{name}' = '{value}')")


def _bronze_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    table: str,
    source_id: str | None = None,
    exclude_source_ids: tuple[str, ...] = (),
) -> DataFrame:
    """A window of bronze, filtered on **both** `ingest_date` and `fetched_at`.

    `ingest_date` is a stored column, not a transform of `fetched_at` — Iceberg can only
    prune partitions from a predicate on the partition column itself. A `fetched_at`-only
    predicate prunes nothing and this job, whose whole point is cheapness, silently scans
    all of bronze (SPEC §10.3).
    """
    from pyspark.sql import functions as F

    since, until = ensure_utc(since), ensure_utc(until)
    query = spark.table(table).where(
        (F.col("ingest_date") >= F.lit(since.date().isoformat()))
        & (F.col("ingest_date") <= F.lit(until.date().isoformat()))
        & (F.col("fetched_at") >= F.lit(since))
        & (F.col("fetched_at") < F.lit(until))
    )
    if source_id is not None:
        query = query.where(F.col("source_id") == source_id)
    if exclude_source_ids:
        query = query.where(~F.col("source_id").isin(list(exclude_source_ids)))
    return query


def _outcome_counts(windowed: DataFrame) -> dict[str, int]:
    # Aliased to `n`, not the default `count`: `Row` subclasses `tuple`, so a column
    # actually named `count` would be shadowed by `tuple.count` on attribute access.
    from pyspark.sql import functions as F

    rows = windowed.groupBy("outcome").agg(F.count("*").alias("n")).collect()
    return {row.outcome: row.n for row in rows}


@dataclass(frozen=True)
class NormalizeResult:
    """What one `normalize_window` run did. SPEC §11's monitoring is built from numbers
    exactly like this one, on the bronze side (`CommitResult`)."""

    bronze_rows: int
    skipped_rows: int  # outcome IN ('error', 'empty') — not parsed, SPEC §6.2 quarantine
    articles_committed: int
    articles_table_rows: int
    rejects_committed: int
    rejects_table_rows: int


def normalize_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    bronze_table: str = BRONZE_TABLE,
    articles_table: str = ARTICLES_TABLE,
    rejects_table: str = PARSE_REJECTS_TABLE,
) -> NormalizeResult:
    """Bronze window -> `silver.articles` + `silver.parse_rejects`, MERGEd.

    Only `outcome='ok'` rows are parsed — an `error` row's payload is error text, and an
    `empty` row's is a bare `null`/zero bytes; SPEC §6.2's quarantine record for both
    already exists in `bronze.raw_documents` itself. They are counted, not parsed
    (`NormalizeResult.skipped_rows`), so a completely stale window is still visible.

    MERGE on `article_id`, `WHEN NOT MATCHED THEN INSERT` only. Nothing in SPEC §9's
    articles schema legitimately changes after first sight — a changed title yields a
    new `article_id`, which is correct: it is a different version of the page, and
    Phase 3's dedup collapses the pair. An UPDATE clause here would be a way to silently
    rewrite history.
    """
    from pyspark.sql import functions as F

    ensure_tables(spark, articles_table=articles_table, rejects_table=rejects_table)

    windowed = _bronze_window(
        spark, since, until, table=bronze_table, exclude_source_ids=NON_ARTICLE_SOURCES
    )
    counts = _outcome_counts(windowed)
    bronze_rows = counts.get("ok", 0)
    skipped_rows = sum(n for outcome, n in counts.items() if outcome != "ok")

    silver = windowed.where(F.col("outcome") == "ok").mapInPandas(
        _normalize_partitions, schema=SILVER_SCHEMA
    )
    clean, rejected = split_rejects(silver)

    # Within-batch duplicates (an overlapping backfill window re-parses the same feed
    # entry twice) would slip past the MERGE's NOT MATCHED clause, which only sees the
    # target — same reasoning as `commit_bronze.commit`'s dropDuplicates on ingest_id.
    # Explicit column order, matching `ARTICLES_DDL`: `MERGE ... INSERT *` is positional.
    # `localCheckpoint` materializes the plan so the MERGE below doesn't inherit a
    # mapInPandas-rooted lineage — see the matching comment on `rejects`.
    articles = (
        clean.dropDuplicates(["article_id"]).select(*_ARTICLES_COLUMNS).localCheckpoint(eager=True)
    )
    articles.createOrReplaceTempView("_new_articles")
    before = spark.table(articles_table).count()
    spark.sql(
        f"""
        MERGE INTO {articles_table} AS target
        USING _new_articles AS source
        ON target.article_id = source.article_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table(articles_table).count()

    rejects = (
        rejected.select("ingest_id", "source_id", "parse_error", "fetched_at")
        .withColumn("rejected_at", F.lit(ensure_utc(utc_now())))
        .dropDuplicates(["ingest_id"])
        # Materializes the plan here rather than leaving it lazy through the MERGE:
        # without this, Catalyst's CollapseProject rule can try to collapse a project
        # across the mapInPandas boundary this DataFrame descends from and produce an
        # unresolved plan (Spark's own validator catches it and raises rather than
        # running a broken query, but the fix is to stop it building that plan at all).
        .localCheckpoint(eager=True)
    )
    rejects.createOrReplaceTempView("_new_rejects")
    rejects_before = spark.table(rejects_table).count()
    spark.sql(
        f"""
        MERGE INTO {rejects_table} AS target
        USING _new_rejects AS source
        ON target.ingest_id = source.ingest_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejects_after = spark.table(rejects_table).count()

    return NormalizeResult(
        bronze_rows=bronze_rows,
        skipped_rows=skipped_rows,
        articles_committed=after - before,
        articles_table_rows=after,
        rejects_committed=rejects_after - rejects_before,
        rejects_table_rows=rejects_after,
    )


HN_COMMENTS_COLUMNS = [
    "item_id",
    "parent_id",
    "by",
    "text",
    "created_at",
    "dead",
    "deleted",
    "fetched_at",
    "ingest_id",
]

HN_COMMENTS_ROW_SCHEMA = """
    item_id string, parent_id string, by string, text string, created_at timestamp,
    dead boolean, deleted boolean, fetched_at timestamp, ingest_id string
"""


def _extract_comments_row(row: dict) -> list[dict]:
    result = get_parser("hackernews")(row["payload"])
    return [
        {
            "item_id": c.item_id,
            "parent_id": c.parent_id,
            "by": c.by,
            "text": c.text,
            "created_at": c.created_at,
            "dead": c.dead,
            "deleted": c.deleted,
            "fetched_at": row["fetched_at"],
            "ingest_id": row.get("ingest_id", ""),
        }
        for c in result.comments
    ]


def _extract_comments_partitions(iterator):
    import pandas as pd

    for pdf in iterator:
        rows = [row for record in pdf.to_dict("records") for row in _extract_comments_row(record)]
        yield pd.DataFrame(rows, columns=HN_COMMENTS_COLUMNS)


def _resolve_story_ids(
    spark: SparkSession,
    new_comments: DataFrame,
    *,
    comments_table: str,
    max_hops: int = 25,
) -> DataFrame:
    """Add `story_id` by walking each new comment's `parent_id` up the comment graph.

    A comment's `parent` is either another comment or the story itself, and HN never
    returns a *story* through this same item-JSON walk — stories become `ParsedItem`s,
    not `ParsedComment`s (`parse/hackernews.py`). So the id where the chain stops being
    a comment we know about *is* the story: no lookup against `silver.articles` needed.

    Comments are immutable once committed here (`WHEN NOT MATCHED THEN INSERT` only, the
    same rule `normalize_window` applies to articles), so this only resolves the
    incoming batch. An ancestor this job has not ingested yet leaves `story_id` pointing
    at the highest ancestor currently known rather than the true root — a real,
    documented limitation of resolving from single-fetch data (see
    docs/runbooks/phase-2.md's velocity finding), not a bug to silently paper over.
    """
    from pyspark.sql import functions as F

    edges = (
        spark.table(comments_table)
        .select("item_id", "parent_id")
        .unionByName(new_comments.select("item_id", "parent_id"))
        .dropDuplicates(["item_id"])
    )
    lookup = edges.withColumnRenamed("item_id", "_id").withColumnRenamed("parent_id", "_parent")

    resolved = new_comments.withColumn("story_id", F.col("parent_id"))
    for hop in range(max_hops):
        resolved = (
            resolved.join(lookup, resolved["story_id"] == lookup["_id"], "left")
            .withColumn("story_id", F.coalesce(F.col("_parent"), F.col("story_id")))
            .drop("_id", "_parent")
        )
        # Every few hops, not every hop: `localCheckpoint` truncates the logical plan
        # back to a leaf, which is what keeps `max_hops` chained joins from either
        # building a plan deep enough for Catalyst's CollapseProject to mis-optimize
        # (see the comment on `rejects` in `normalize_window`) or just getting slow to
        # analyze — but checkpointing on every hop would mean 25 materializations for
        # what is, in practice, usually a 2-3 level comment thread.
        if hop % 5 == 4:
            resolved = resolved.localCheckpoint(eager=True)
    return resolved.localCheckpoint(eager=True)


@dataclass(frozen=True)
class HnCommentsResult:
    hackernews_rows: int
    comments_extracted: int
    comments_committed: int
    table_rows: int


def normalize_hn_comments_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    bronze_table: str = BRONZE_TABLE,
    comments_table: str = HN_COMMENTS_TABLE,
) -> HnCommentsResult:
    """Bronze window, `hackernews` partitions only -> `silver.hn_comments`, MERGEd.

    A second pass rather than folding into `normalize_window`: bronze is partitioned by
    `source_id`, so this reads only the `hackernews` partitions, and each pass keeps one
    output schema instead of a discriminator column nothing else needs.
    """
    from pyspark.sql import functions as F

    ensure_tables(spark, comments_table=comments_table)

    windowed = _bronze_window(spark, since, until, table=bronze_table, source_id="hackernews")
    hackernews_rows = _outcome_counts(windowed).get("ok", 0)

    extracted = windowed.where(F.col("outcome") == "ok").mapInPandas(
        _extract_comments_partitions, schema=HN_COMMENTS_ROW_SCHEMA
    )
    extracted = extracted.dropDuplicates(["item_id"])
    comments_extracted = extracted.count()

    resolved = _resolve_story_ids(spark, extracted, comments_table=comments_table)
    # Explicit column order, matching `HN_COMMENTS_DDL` exactly: `MERGE ... INSERT *`
    # is positional, and `_resolve_story_ids` appends `story_id` as the last column,
    # not the third.
    resolved = resolved.select(
        "item_id",
        "parent_id",
        "story_id",
        "by",
        "text",
        "created_at",
        "fetched_at",
        "ingest_id",
        "dead",
        "deleted",
    )
    resolved.createOrReplaceTempView("_new_comments")
    before = spark.table(comments_table).count()
    spark.sql(
        f"""
        MERGE INTO {comments_table} AS target
        USING _new_comments AS source
        ON target.item_id = source.item_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table(comments_table).count()

    return HnCommentsResult(
        hackernews_rows=hackernews_rows,
        comments_extracted=comments_extracted,
        comments_committed=after - before,
        table_rows=after,
    )


# --------------------------------------------------------------------------------------
# Third pass: `hn_scores` bronze partitions -> `silver.hn_score_snapshots`. SPEC §7.4.
# --------------------------------------------------------------------------------------

HN_SCORES_COLUMNS = ["item_id", "score", "descendants", "title", "observed_at", "ingest_id"]

HN_SCORES_ROW_SCHEMA = """
    item_id string, score int, descendants int, title string,
    observed_at timestamp, ingest_id string
"""


def _extract_scores_row(row: dict) -> list[dict]:
    result = get_parser("hn_scores")(row["payload"])
    return [
        {
            "item_id": s.item_id,
            "score": s.score,
            "descendants": s.descendants,
            "title": s.title,
            # The envelope's timestamp, not the payload's. See `HN_SCORES_DDL`.
            "observed_at": row["fetched_at"],
            "ingest_id": row.get("ingest_id", ""),
        }
        for s in result.score_snapshots
    ]


def _extract_scores_partitions(iterator):
    import pandas as pd

    for pdf in iterator:
        rows = [row for record in pdf.to_dict("records") for row in _extract_scores_row(record)]
        yield pd.DataFrame(rows, columns=HN_SCORES_COLUMNS)


@dataclass(frozen=True)
class HnScoresResult:
    hn_scores_rows: int
    snapshots_extracted: int
    snapshots_committed: int
    table_rows: int


def normalize_hn_scores_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    bronze_table: str = BRONZE_TABLE,
    scores_table: str = HN_SCORES_TABLE,
) -> HnScoresResult:
    """Bronze window, `hn_scores` partitions only -> `silver.hn_score_snapshots`, MERGEd.

    A third pass, for the same reason the comments pass is a second one: bronze partitions
    by `source_id`, so this reads only what it needs and keeps one output schema.

    **MERGEd on `ingest_id`, not on `item_id`.** The other two passes dedupe on the
    document's own id because a document is a thing that exists once. A snapshot is a
    *measurement*, and the same story measured an hour later is a new row, not a duplicate
    of the old one — deduping on `item_id` here would keep exactly one score per story and
    delete the slope this table exists to provide. `ingest_id` is unique per fetch, so
    replaying a committed window still inserts nothing (SPEC §6.3).
    """
    from pyspark.sql import functions as F

    ensure_tables(spark, scores_table=scores_table)

    windowed = _bronze_window(spark, since, until, table=bronze_table, source_id="hn_scores")
    hn_scores_rows = _outcome_counts(windowed).get("ok", 0)

    extracted = windowed.where(F.col("outcome") == "ok").mapInPandas(
        _extract_scores_partitions, schema=HN_SCORES_ROW_SCHEMA
    )
    extracted = extracted.dropDuplicates(["ingest_id"])
    snapshots_extracted = extracted.count()

    extracted = extracted.select(*HN_SCORES_COLUMNS)
    extracted.createOrReplaceTempView("_new_score_snapshots")
    before = spark.table(scores_table).count()
    spark.sql(
        f"""
        MERGE INTO {scores_table} AS target
        USING _new_score_snapshots AS source
        ON target.ingest_id = source.ingest_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table(scores_table).count()

    return HnScoresResult(
        hn_scores_rows=hn_scores_rows,
        snapshots_extracted=snapshots_extracted,
        snapshots_committed=after - before,
        table_rows=after,
    )
