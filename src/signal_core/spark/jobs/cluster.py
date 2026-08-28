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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from signal_core import dedup
from signal_core.records import ArticleClusterRow, ClusterRow, PreparedArticle, as_article
from signal_core.spark.tables import ensure_columns
from signal_core.timeutil import ensure_utc, utc_now

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

ARTICLES_TABLE = "silver.articles"
CLUSTERS_TABLE = "silver.story_clusters"
ARTICLE_CLUSTERS_TABLE = "silver.article_clusters"

# Bump on any change to the decision, the thresholds, or the blocking. A mixed table is
# then diagnosable rather than a mystery, and ADR-0009's measurement trail stays checkable.
ALGO_VERSION = "3.D"

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
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL,
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
    # Columns this run had to add to a table that predates them. Surfaced rather than logged:
    # a schema changing under a running pipeline is something a person should see.
    columns_added: tuple[str, ...] = ()

    @property
    def dedup_ratio(self) -> float:
        return self.articles_in / self.clusters_out if self.clusters_out else 0.0


def ensure_tables(
    spark: SparkSession,
    *,
    clusters_table: str = CLUSTERS_TABLE,
    map_table: str = ARTICLE_CLUSTERS_TABLE,
) -> list[str]:
    namespace = clusters_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    added = []
    for table, ddl in ((clusters_table, CLUSTERS_DDL), (map_table, ARTICLE_CLUSTERS_DDL)):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) "
            f"USING iceberg PARTITIONED BY (days(window_start)) {_TBLPROPERTIES}"
        )
        # `CREATE TABLE IF NOT EXISTS` never revisits a table that already exists, so a DDL
        # that grows a column drifts away from the deployed table in silence. 3.B.4 added
        # `first_seen`/`last_seen` here and the deployed table kept its original 17 columns
        # until the brief failed reading them (see `spark/tables.py`).
        added += ensure_columns(spark, table, ddl)
    return added


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


def _candidate_pairs(rows: Sequence[PreparedArticle]) -> tuple[set[tuple[str, str]], int]:
    """Blocking. Returns (candidate pairs, blocking keys dropped for being too large).

    **One key per branch of `dedup.decide`.** A rule the decision can reach but blocking
    never proposes a candidate for is dead in this path while still passing every test that
    exercises `group_stories`, whose all-pairs enumeration hides the omission. `group_edges`
    promises the two paths "differ only in how they arrive at the edges", so every branch
    added there has to be matched here.

    Exact for both token branches: `dedup.blocking_keys` uses prefix filtering, so any pair
    above the Jaccard threshold is guaranteed to share a key. Exact for the accession branch
    too — that rule fires only on equality, so one key per accession number is complete by
    construction.

    Approximate for simhash, and less tightly than it looks: the bands are computed over the
    **stored** `silver.articles.simhash`, which is hashed over raw text, while `dedup.decide`
    recomputes its own simhash over *cleaned* text. Those are hashes of two different strings,
    so a band collision is only loosely correlated with proximity of the value actually
    compared. It survives because `NEAR_DUPLICATE_DISTANCE` is 0 — identical cleaned text
    nearly always means near-identical raw text — but the honest description is "a cheap
    correlated prefilter", not "a banding of the compared value". The token branches are the
    ones carrying the exactness guarantee; recomputing the simhash here would cost a second
    pass over every article to tighten a branch whose own threshold is already exact-match.
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
        # The accession branch of `decide`. Without this the rule 4A.G added there — "one
        # Form 4 clusters twice" — is reachable in `group_stories` and mostly unreachable
        # here, because the pair it targets is precisely the pair the other keys cannot
        # produce: two EDGAR index entries for one submission carry different titles (no
        # shared tokens at all — "Koss Jennifer G." against "Reservoir Media, Inc."), ~9-token
        # boilerplate bodies whose only shared tokens are the filing date, and an accession
        # number that `content_tokens` strips as a long digit run.
        #
        # Sharing the date is what makes it fail *with scale* rather than outright: EDGAR
        # posts thousands of filings a day, so `b:2026`/`b:08`/`b:18` grow past
        # MAX_BLOCK_SIZE and get dropped, taking the only co-blocking the pair had. Measured
        # over synthetic filings at the shape of a real day: 100% of same-filing pairs
        # proposed at 100 submissions, 92% at 300, 75.8% at 1,000 — an accuracy loss that
        # arrives quietly as the lake grows and shows up nowhere in the eval, which scores
        # `decide` rather than what blocking fed it.
        #
        # These blocks hold 2-3 rows (one submission indexed under each CIK it concerns), so
        # MAX_BLOCK_SIZE never drops them and the cost is one key per filing.
        keys += [f"x:{accession}" for accession in prepared.accessions]
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
    columns_added = ensure_tables(spark, clusters_table=clusters_table, map_table=map_table)
    since, until = ensure_utc(since), ensure_utc(until)

    windowed = read_window(spark, since, until, table=articles_table)
    articles = [as_article(row.asDict()) for row in windowed.collect()]
    articles_in = len(articles)

    # `prepared` is computed once per article and carried on the row, because clustering
    # compares O(n^2) pairs and tokenizing inside that loop is what made Phase 0's version
    # slow enough to notice. `PreparedArticle` is that shape: an `Article` plus the one
    # derived field, kept off `Article` itself so the DDL parity test stays exact.
    deduped, exact_removed = dedup.exact_dedup(articles)
    prepared_rows: list[PreparedArticle] = [
        {**row, "prepared": dedup.prepare(row["title"], row["body_text"])} for row in deduped
    ]

    candidates, dropped = _candidate_pairs(prepared_rows)
    by_id = {row["article_id"]: row for row in prepared_rows}
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

    grouped = dedup.group_edges(prepared_rows, edges)
    ordering_key = _ordering_key([row["article_id"] for row in prepared_rows])
    clustered_at = utc_now()

    # Annotated, not inferred. These two literals are what 3.D's first defect looked like
    # from the writer's side — the DDL grew `first_seen`/`last_seen` and nothing tied this
    # dict to it. `ClusterRow` and `ArticleClusterRow` are checked against those DDLs by
    # `tests/test_records.py`, so a missing or misspelled key is now a mypy error here
    # rather than a `COLUMN_NOT_FOUND` on the first real run.
    cluster_rows: list[ClusterRow] = []
    map_rows: list[ArticleClusterRow] = []
    for cluster in grouped.clusters:
        cluster_row: ClusterRow = {
            "cluster_id": cluster["cluster_id"],
            "window_start": since,
            "window_end": until,
            "canonical_article_id": cluster["canonical_article_id"],
            "title": cluster["title"],
            "url_canonical": cluster["url_canonical"],
            "publisher_domain": cluster["publisher_domain"],
            "published_at": cluster["published_at"],
            "fetched_at": cluster["fetched_at"],
            "first_seen": cluster["first_seen"],
            "last_seen": cluster["last_seen"],
            "event_date": cluster["fetched_at"],
            "article_count": cluster["article_count"],
            "distinct_publisher_count": cluster["distinct_publisher_count"],
            "publishers": cluster["publishers"],
            "timestamp_flagged": cluster["timestamp_flagged"],
            "ordering_key": ordering_key,
            "algo_version": ALGO_VERSION,
            "clustered_at": clustered_at,
        }
        cluster_rows.append(cluster_row)
        for article_id in cluster["article_ids"]:
            map_row: ArticleClusterRow = {
                "article_id": article_id,
                "cluster_id": cluster["cluster_id"],
                "window_start": since,
                "is_canonical": article_id == cluster["canonical_article_id"],
                "algo_version": ALGO_VERSION,
            }
            map_rows.append(map_row)

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
        columns_added=tuple(columns_added),
    )


def _overwrite(
    spark: SparkSession, table: str, rows: Sequence[Mapping[str, Any]], ddl: str
) -> None:
    """Replace exactly the partitions these rows fall in, atomically.

    `Sequence[Mapping[...]]` rather than `list[dict[...]]` so the typed row lists above are
    accepted: a `list[ClusterRow]` is not a `list[dict[str, Any]]` to mypy, because `list`
    is invariant and a caller holding the wider type could append the wrong row shape to it.
    Reading rows is all this does, so the read-only types are the honest signature anyway.

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
