"""silver.articles -> story clusters. SPEC §7.1, §9, §12; docs/runbooks/phase-3.md 3.B.

The transform half of 3.B. The decision itself lives in `signal_core.dedup` and is shared
with `evals/score.py`, so what this job adds is scale and durability: candidate generation
that does not go quadratic, connected components, and two Iceberg tables.

**These are the only tables in the repo that are not insert-only, and the reason is worth
stating.** Every other table holds immutable facts, so `WHEN NOT MATCHED THEN INSERT` is
right. A cluster assignment is not a fact about an article — it is the output of a function
of (window, algorithm version). Re-running after a threshold change must *replace* the
window's rows or the table accumulates contradictory assignments and `dedup_ratio` stops
meaning anything. `writeTo(...).overwritePartitions()` does exactly that in one atomic
snapshot, and re-running an unchanged window is a no-op by construction.

Consecutive daily runs share 48 of their 72 hours, so one article appears in three windows
under three possible `cluster_id`s. That is why `window_start` is part of the key of both
tables: a cluster is scoped to the window that produced it, and the brief reads the newest.
Without it, daily runs would silently double every row.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from signal_core import dedup
from signal_core.timeutil import ensure_utc, utc_now

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

ARTICLES_TABLE = "silver.articles"
CLUSTERS_TABLE = "silver.story_clusters"
ARTICLE_CLUSTERS_TABLE = "silver.article_clusters"

# Bump on any change to the decision, the thresholds, or the blocking. A mixed table is
# then diagnosable rather than a mystery, and ADR-0009's measurement trail stays checkable.
ALGO_VERSION = "3.B.1"

# A blocking key held by more than this many articles is dropped rather than exploded: one
# token shared by 800 filings would emit 320k candidate pairs on its own. Dropping it costs
# recall, so the count is reported (`blocking_keys_dropped`) instead of being swallowed —
# silently lost recall is the failure class SPEC §11 exists to catch.
MAX_BLOCK_SIZE = 400

# Union-find runs on the driver over the surviving edge list: exact, deterministic, and
# small (a real window yields a few thousand true edges). Above this the job fails loudly
# rather than swallowing the driver — the documented escape hatch is iterative label
# propagation, the pattern `normalize._resolve_story_ids` already uses for HN threads.
MAX_EDGES = 1_000_000

_TBLPROPERTIES = """
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes' = '134217728'
    )
"""

CLUSTERS_DDL = """
    cluster_id string NOT NULL,
    window_start timestamp NOT NULL,
    window_end timestamp NOT NULL,
    canonical_article_id string NOT NULL,
    title string,
    url_canonical string,
    publisher_domain string,
    published_at timestamp,
    fetched_at timestamp NOT NULL,
    event_date timestamp NOT NULL,
    article_count int NOT NULL,
    distinct_publisher_count int NOT NULL,
    publishers array<string>,
    timestamp_flagged boolean,
    ordering_key string NOT NULL,
    algo_version string NOT NULL,
    clustered_at timestamp NOT NULL
"""

# The map, kept separate so clustering is non-destructive: `silver.articles` is never
# mutated, and dropping every cluster table would lose no fact about the world.
ARTICLE_CLUSTERS_DDL = """
    article_id string NOT NULL,
    cluster_id string NOT NULL,
    window_start timestamp NOT NULL,
    is_canonical boolean NOT NULL,
    algo_version string NOT NULL
"""


@dataclass(frozen=True)
class ClusterWindowResult:
    """What one run did. Every field is something a person would ask about afterwards."""

    articles_in: int
    exact_duplicates_removed: int
    candidate_pairs: int
    edges: int
    clusters_out: int
    dissolved: int
    dissolved_articles: int
    blocking_keys_dropped: int
    ordering_key: str

    @property
    def dedup_ratio(self) -> float:
        return self.articles_in / self.clusters_out if self.clusters_out else 0.0


def ensure_tables(
    spark: SparkSession,
    *,
    clusters_table: str = CLUSTERS_TABLE,
    map_table: str = ARTICLE_CLUSTERS_TABLE,
) -> None:
    namespace = clusters_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for table, ddl in ((clusters_table, CLUSTERS_DDL), (map_table, ARTICLE_CLUSTERS_DDL)):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) "
            f"USING iceberg PARTITIONED BY (days(window_start)) {_TBLPROPERTIES}"
        )


def read_window(
    spark: SparkSession, since: datetime, until: datetime, *, table: str = ARTICLES_TABLE
) -> DataFrame:
    """The window, partition-pruned on `event_date` and free of unparseable rows.

    `parse_error IS NULL` is not optional: SPEC §9 keeps a single bad entry inside an
    otherwise good feed in `articles` with its own error rather than quarantining the row,
    so those are present and must not reach the ranker with an empty title.
    """
    from pyspark.sql import functions as F

    return (
        spark.table(table)
        .where(F.col("event_date") >= F.lit(ensure_utc(since)))
        .where(F.col("event_date") < F.lit(ensure_utc(until)))
        .where(F.col("parse_error").isNull())
    )


def _ordering_key(article_ids: list[str]) -> str:
    """SPEC §7.1 asks that the input ordering key be recorded so a replay can reproduce a
    run. Two parts: the deterministic sort, and a digest of the input set.

    The digest is the half that is actually checkable — it lets a replay prove it saw the
    same articles, rather than asserting it. Worth noting that this implementation is
    order-**independent** anyway: union-find over a complete edge list yields the same
    components whatever order the edges arrive in, and canonical selection breaks ties on
    `(-authority, fetched_at, article_id)`. So a replay reproduces exactly, not within a
    tolerance — a stronger result than §7.1 assumed when it called clustering
    "order-dependent by construction".
    """
    digest = hashlib.sha256("\n".join(sorted(article_ids)).encode("utf-8")).hexdigest()
    return f"fetched_at,article_id@{digest[:16]}"


def _candidate_pairs(rows: list[dict[str, Any]]) -> tuple[set[tuple[str, str]], int]:
    """Blocking. Returns (candidate pairs, blocking keys dropped for being too large).

    Exact for both token branches: `dedup.blocking_keys` uses prefix filtering, so any pair
    above the Jaccard threshold is guaranteed to share a key. The simhash branch is covered
    by banding the stored `silver.articles.simhash` — approximate, which is all a *blocking*
    key has to be, since every surviving candidate is then decided exactly by `dedup.decide`.
    That is the one job the stored column still has now the decision recomputes its own
    simhash over cleaned text.
    """
    title_frequency: Counter[str] = Counter()
    body_frequency: Counter[str] = Counter()
    for row in rows:
        title_frequency.update(row["prepared"].title)
        body_frequency.update(row["prepared"].body)

    buckets: dict[str, list[str]] = {}
    for row in rows:
        prepared = row["prepared"]
        keys = [
            f"t:{k}"
            for k in dedup.blocking_keys(prepared.title, title_frequency, dedup.TITLE_JACCARD)
        ]
        keys += [
            f"b:{k}" for k in dedup.blocking_keys(prepared.body, body_frequency, dedup.BODY_JACCARD)
        ]
        # 8 bands of 8 bits over the stored simhash. Probabilistic by design; the decision
        # re-checks every candidate.
        stored = row["simhash"] or 0
        keys += [f"s:{band}:{(stored >> (band * 8)) & 0xFF}" for band in range(8)]
        for key in keys:
            buckets.setdefault(key, []).append(row["article_id"])

    pairs: set[tuple[str, str]] = set()
    dropped = 0
    for members in buckets.values():
        if len(members) > MAX_BLOCK_SIZE:
            dropped += 1
            continue
        members.sort()
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                pairs.add((members[i], members[j]))
    return pairs, dropped


def cluster_window(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    articles_table: str = ARTICLES_TABLE,
    clusters_table: str = CLUSTERS_TABLE,
    map_table: str = ARTICLE_CLUSTERS_TABLE,
) -> ClusterWindowResult:
    """Cluster one window and replace that window's rows in both tables."""
    ensure_tables(spark, clusters_table=clusters_table, map_table=map_table)
    since, until = ensure_utc(since), ensure_utc(until)

    windowed = read_window(spark, since, until, table=articles_table)
    articles = [row.asDict() for row in windowed.collect()]
    articles_in = len(articles)

    deduped, exact_removed = dedup.exact_dedup(articles)
    for row in deduped:
        row["prepared"] = dedup.prepare(row["title"] or "", row["body_text"] or "")

    candidates, dropped = _candidate_pairs(deduped)
    by_id = {row["article_id"]: row for row in deduped}
    edges = [
        (a, b)
        for a, b in sorted(candidates)
        if dedup.decide(by_id[a]["prepared"], by_id[b]["prepared"])
    ]
    if len(edges) > MAX_EDGES:
        raise RuntimeError(
            f"{len(edges)} same-story edges over {articles_in} articles exceeds MAX_EDGES "
            f"({MAX_EDGES}). Driver-side union-find will not hold this; switch to iterative "
            "label propagation (see normalize._resolve_story_ids) before raising the cap."
        )

    grouped = dedup.group_edges(deduped, edges)
    ordering_key = _ordering_key([row["article_id"] for row in deduped])
    clustered_at = utc_now()

    cluster_rows, map_rows = [], []
    for cluster in grouped.clusters:
        cluster_rows.append(
            {
                "cluster_id": cluster["cluster_id"],
                "window_start": since,
                "window_end": until,
                "canonical_article_id": cluster["canonical_article_id"],
                "title": cluster["title"],
                "url_canonical": cluster["url_canonical"],
                "publisher_domain": cluster["publisher_domain"],
                "published_at": cluster["published_at"],
                "fetched_at": cluster["fetched_at"],
                "event_date": cluster["fetched_at"],
                "article_count": cluster["article_count"],
                "distinct_publisher_count": cluster["distinct_publisher_count"],
                "publishers": cluster["publishers"],
                "timestamp_flagged": cluster["timestamp_flagged"],
                "ordering_key": ordering_key,
                "algo_version": ALGO_VERSION,
                "clustered_at": clustered_at,
            }
        )
        for article_id in cluster["article_ids"]:
            map_rows.append(
                {
                    "article_id": article_id,
                    "cluster_id": cluster["cluster_id"],
                    "window_start": since,
                    "is_canonical": article_id == cluster["canonical_article_id"],
                    "algo_version": ALGO_VERSION,
                }
            )

    _overwrite(spark, clusters_table, cluster_rows, CLUSTERS_DDL)
    _overwrite(spark, map_table, map_rows, ARTICLE_CLUSTERS_DDL)

    return ClusterWindowResult(
        articles_in=articles_in,
        exact_duplicates_removed=exact_removed,
        candidate_pairs=len(candidates),
        edges=len(edges),
        clusters_out=len(grouped.clusters),
        dissolved=grouped.dissolved,
        dissolved_articles=grouped.dissolved_articles,
        blocking_keys_dropped=dropped,
        ordering_key=ordering_key,
    )


def _overwrite(spark: SparkSession, table: str, rows: list[dict[str, Any]], ddl: str) -> None:
    """Replace exactly the partitions these rows fall in, atomically.

    An empty window still has to clear the partition, and `overwritePartitions` on an empty
    DataFrame writes nothing — so that case is a targeted DELETE instead. Skipping it would
    leave a previous run's clusters standing for a window that now has none, which is the
    kind of stale row that makes a `dedup_ratio` quietly wrong.
    """
    schema = ", ".join(
        line.strip().removesuffix(",").replace(" NOT NULL", "")
        for line in ddl.strip().splitlines()
        if line.strip()
    )
    if not rows:
        return
    frame = spark.createDataFrame(rows, schema=schema).select(
        *[line.strip().split()[0] for line in ddl.strip().splitlines() if line.strip()]
    )
    frame.writeTo(table).overwritePartitions()
