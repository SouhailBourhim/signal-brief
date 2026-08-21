"""silver.articles -> entity mentions and a bitemporal entity dimension. SPEC §7.2, §9;
docs/runbooks/phase-3.md 3.C.

The transform half of 3.C. The decision lives in `signal_core.entities.resolve` and is shared
with `evals/score.py`; detection lives in `signal_core.entities.mentions` and is shared with
`evals/sample_mentions.py`. What this job adds is scale, two tables, and the one thing
neither of those modules can express: **time**.

## Two tables, two different relationships with time, and neither is an accident

`silver.entity_mentions` is a **function of (article, dictionary, algorithm)**, none of which
is a fact about the world the way a bronze document is. Re-resolving an article after a
dictionary rebuild must *replace* its mentions, or the table accumulates contradictory
answers and every count over it becomes meaningless. So it is partitioned by the article's
`event_date` and written with `overwritePartitions`, exactly like `silver.story_clusters` and
for the same reason.

Note what it is *not* partitioned by: the resolve window. A cluster genuinely belongs to the
window that produced it — the same article clusters differently in overlapping windows — but
a mention does not. `Apple` at character 41 of one article resolves the same way whichever
window asks, so keying mentions by window would store the same answer three times and invite
three different ones.

`silver.dim_entities` is **SCD2**, and it is the one table in this repo where a row is
superseded rather than replaced. SPEC §7.2 names the reason: "companies that rename — which
is an SCD2 problem wearing a disguise". When Facebook, Inc. became Meta Platforms, Inc., an
article published the day before did not retroactively become an article about Meta. The
labeled set encodes that rule too: a renamed company is labeled with the entity valid **at
the article's publication date**. A dimension that simply overwrote `canonical_name` could
not answer that question, and the labels would be unscoreable against it.

So a load closes the outgoing row (`valid_to`, `is_current = false`) and opens a new one
rather than updating in place, and the dictionary snapshot's `built_at` is what supplies the
boundary — which is the concrete reason `dictionary.py` commits a snapshot with a timestamp
instead of fetching SEC live. A live lookup has no `valid_from` to give.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from signal_core.entities import dictionary as dict_module
from signal_core.entities import mentions as mention_module
from signal_core.entities import resolve as resolve_module
from signal_core.timeutil import ensure_utc, utc_now

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

ARTICLES_TABLE = "silver.articles"
MENTIONS_TABLE = "silver.entity_mentions"
ENTITIES_TABLE = "silver.dim_entities"

# Bump on any change to detection, the channels, or the fitted constants. A mixed table is
# then diagnosable rather than a mystery — and unlike clustering, mentions from two algorithm
# versions can coexist in one partition until it is next rewritten, so the column is the only
# way to tell them apart.
ALGO_VERSION = "3.C"

# The far-future sentinel for an open SCD2 interval. A NULL `valid_to` would be more
# fashionable and is worse to query: `valid_from <= t AND t < valid_to` is one predicate that
# a reader gets right, while the NULL form needs a COALESCE nobody remembers on the first
# attempt. `is_current` is carried alongside for the same reason — it is redundant with
# `valid_to`, and it is the column people actually filter on.
VALID_TO_OPEN = datetime(9999, 12, 31, tzinfo=None)

_TBLPROPERTIES = """
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes' = '134217728'
    )
"""

MENTIONS_DDL = """
    mention_id string NOT NULL,
    article_id string NOT NULL,
    surface_form string NOT NULL,
    char_start int NOT NULL,
    char_end int NOT NULL,
    entity_id string,
    confidence double NOT NULL,
    resolution_method string NOT NULL,
    unlinked_reason string,
    matched_alias string,
    event_date timestamp NOT NULL,
    dictionary_built_at string NOT NULL,
    algo_version string NOT NULL,
    resolved_at timestamp NOT NULL
"""

# SPEC §9's `dim_entities`, plus the provenance columns that make a rename readable after the
# fact: which snapshot introduced the row, and which source claimed it.
ENTITIES_DDL = """
    entity_id string NOT NULL,
    canonical_name string NOT NULL,
    ticker string,
    cik string,
    entity_type string NOT NULL,
    source string NOT NULL,
    parent_entity_id string,
    valid_from timestamp NOT NULL,
    valid_to timestamp NOT NULL,
    is_current boolean NOT NULL
"""


@dataclass(frozen=True)
class ResolveWindowResult:
    """What one run did. Every field is something a person would ask about afterwards, and
    the unlinked breakdown is the one that makes a bad run visible: SPEC §7.2's floor means
    a broken dictionary shows up as a surge in `no-such-entity`, not as an error."""

    articles_in: int
    mentions_detected: int
    mentions_linked: int
    distinct_entities: int
    by_reason: dict[str, int]
    dictionary_built_at: str

    @property
    def link_rate(self) -> float:
        return self.mentions_linked / self.mentions_detected if self.mentions_detected else 0.0


@dataclass(frozen=True)
class EntityLoadResult:
    """What a dimension load did. `superseded` is the number that matters — it is companies
    that renamed, and a load that reports thousands means the dictionary changed shape rather
    than the world changing."""

    snapshot_entities: int
    inserted: int
    superseded: int
    unchanged: int
    valid_from: datetime


def ensure_tables(
    spark: SparkSession,
    *,
    mentions_table: str = MENTIONS_TABLE,
    entities_table: str = ENTITIES_TABLE,
) -> None:
    namespace = mentions_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {mentions_table} ({MENTIONS_DDL}) "
        f"USING iceberg PARTITIONED BY (days(event_date)) {_TBLPROPERTIES}"
    )
    # Unpartitioned on purpose: SPEC §9 leaves the small dimensions unpartitioned, and this
    # one is ~8k rows plus however many renames history accumulates.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {entities_table} ({ENTITIES_DDL}) "
        f"USING iceberg {_TBLPROPERTIES}"
    )


def read_window(
    spark: SparkSession, since: datetime, until: datetime, *, table: str = ARTICLES_TABLE
) -> DataFrame:
    """The window, partition-pruned on `event_date` and free of unparseable rows.

    `parse_error IS NULL` is not optional, for the same reason `cluster.read_window` says so:
    SPEC §9 keeps a single bad entry inside an otherwise good feed in `articles` with its own
    error, and running a proper-noun detector over a failed parse finds spans in an error
    message.
    """
    from pyspark.sql import functions as F

    return (
        spark.table(table)
        .where(F.col("event_date") >= F.lit(ensure_utc(since)))
        .where(F.col("event_date") < F.lit(ensure_utc(until)))
        .where(F.col("parse_error").isNull())
    )


def resolve_article(
    article: dict[str, Any], dictionary: dict_module.Dictionary
) -> list[dict[str, Any]]:
    """Every mention in one article, resolved. Pure, and the unit the tests exercise."""
    text = mention_module.mention_text(article.get("title"), article.get("body_text"))
    rows = []
    for mention in mention_module.detect(text):
        context = mention_module.context_window(text, mention.char_start, mention.char_end)
        resolution = resolve_module.resolve(mention.surface_form, context, dictionary=dictionary)
        rows.append(
            {
                "mention_id": mention_module.mention_id(article["article_id"], mention.char_start),
                "article_id": article["article_id"],
                "surface_form": mention.surface_form,
                "char_start": mention.char_start,
                "char_end": mention.char_end,
                "entity_id": resolution.entity_id,
                "confidence": resolution.confidence,
                "resolution_method": resolution.method,
                "unlinked_reason": resolution.reason,
                "matched_alias": resolution.matched_alias,
                "event_date": article["event_date"],
            }
        )
    return rows


def resolve_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    articles_table: str = ARTICLES_TABLE,
    mentions_table: str = MENTIONS_TABLE,
    dictionary_path: Path | None = None,
) -> ResolveWindowResult:
    """Resolve one window's articles and replace those days' rows in `entity_mentions`."""
    ensure_tables(spark, mentions_table=mentions_table)
    since, until = ensure_utc(since), ensure_utc(until)
    dictionary = dict_module.load(dictionary_path)

    windowed = read_window(spark, since, until, table=articles_table)
    articles = [row.asDict() for row in windowed.collect()]
    resolved_at = utc_now()

    rows: list[dict[str, Any]] = []
    for article in articles:
        for row in resolve_article(article, dictionary):
            row["dictionary_built_at"] = dictionary.built_at
            row["algo_version"] = ALGO_VERSION
            row["resolved_at"] = resolved_at
            rows.append(row)

    by_reason: dict[str, int] = {}
    for row in rows:
        if row["unlinked_reason"]:
            by_reason[row["unlinked_reason"]] = by_reason.get(row["unlinked_reason"], 0) + 1

    _overwrite(spark, mentions_table, rows, MENTIONS_DDL)
    linked = [row for row in rows if row["entity_id"]]
    return ResolveWindowResult(
        articles_in=len(articles),
        mentions_detected=len(rows),
        mentions_linked=len(linked),
        distinct_entities=len({row["entity_id"] for row in linked}),
        by_reason=dict(sorted(by_reason.items())),
        dictionary_built_at=dictionary.built_at,
    )


def _scd2_row(entity: dict_module.Entity, valid_from: datetime) -> dict[str, Any]:
    return {
        "entity_id": entity.entity_id,
        "canonical_name": entity.canonical_name,
        "ticker": entity.ticker,
        "cik": entity.cik,
        "entity_type": entity.entity_type,
        "source": entity.source,
        "parent_entity_id": entity.parent_entity_id,
        "valid_from": valid_from,
        "valid_to": VALID_TO_OPEN,
        "is_current": True,
    }


# What a change to any of these means the company is, in some way we care about, a different
# entity than the row on file — so the old row closes and a new one opens. `rank` and
# `aliases` are deliberately absent: SEC reorders its file constantly and Wikidata gains
# aliases weekly, and treating either as a rename would fill the dimension with history that
# records nothing about the world.
SCD2_TRACKED = ("canonical_name", "ticker", "cik", "entity_type", "parent_entity_id")


def load_entities(
    spark: SparkSession,
    *,
    entities_table: str = ENTITIES_TABLE,
    dictionary_path: Path | None = None,
    valid_from: datetime | None = None,
) -> EntityLoadResult:
    """Load the dictionary snapshot into `dim_entities` as SCD2. SPEC §7.2, §9.

    Idempotent: loading the same snapshot twice supersedes nothing, because nothing tracked
    differs. That is what makes a re-run safe and what makes `superseded` meaningful when it
    is non-zero.

    `valid_from` defaults to the snapshot's own `built_at` rather than to now. The boundary
    being asserted is "this is what SEC said when the snapshot was taken", and dating it from
    the load would make a dimension whose history depends on when somebody happened to run a
    job.
    """
    ensure_tables(spark, entities_table=entities_table)
    dictionary = dict_module.load(dictionary_path)
    boundary = (
        ensure_utc(valid_from)
        if valid_from
        else ensure_utc(datetime.fromisoformat(dictionary.built_at))
    )

    existing = {
        row["entity_id"]: row.asDict()
        for row in spark.table(entities_table).where("is_current").collect()
    }

    to_insert: list[dict[str, Any]] = []
    to_close: list[str] = []
    unchanged = 0
    for entity in dictionary.entities.values():
        current = existing.get(entity.entity_id)
        if current is None:
            to_insert.append(_scd2_row(entity, boundary))
            continue
        if all(current.get(field) == getattr(entity, field) for field in SCD2_TRACKED):
            unchanged += 1
            continue
        to_close.append(entity.entity_id)
        to_insert.append(_scd2_row(entity, boundary))

    if to_close:
        # Close the outgoing interval where the new one opens, so `valid_from <= t <
        # valid_to` has no gap and no overlap at the boundary instant.
        ids = ", ".join(f"'{entity_id}'" for entity_id in to_close)
        spark.sql(
            f"UPDATE {entities_table} SET valid_to = TIMESTAMP '{boundary.replace(tzinfo=None)}', "
            f"is_current = false WHERE is_current AND entity_id IN ({ids})"
        )

    if to_insert:
        _append(spark, entities_table, to_insert, ENTITIES_DDL)

    return EntityLoadResult(
        snapshot_entities=len(dictionary.entities),
        inserted=len(to_insert),
        superseded=len(to_close),
        unchanged=unchanged,
        valid_from=boundary,
    )


def _schema(ddl: str) -> str:
    return ", ".join(
        line.strip().removesuffix(",").replace(" NOT NULL", "")
        for line in ddl.strip().splitlines()
        if line.strip()
    )


def _columns(ddl: str) -> list[str]:
    return [line.strip().split()[0] for line in ddl.strip().splitlines() if line.strip()]


def _overwrite(spark: SparkSession, table: str, rows: list[dict[str, Any]], ddl: str) -> None:
    """Replace exactly the partitions these rows fall in, atomically."""
    if not rows:
        return
    frame = spark.createDataFrame(rows, schema=_schema(ddl)).select(*_columns(ddl))
    frame.writeTo(table).overwritePartitions()


def _append(spark: SparkSession, table: str, rows: list[dict[str, Any]], ddl: str) -> None:
    if not rows:
        return
    frame = spark.createDataFrame(rows, schema=_schema(ddl)).select(*_columns(ddl))
    frame.writeTo(table).append()
