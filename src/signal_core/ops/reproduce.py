"""The reproducibility harness. SPEC §12's 4B acceptance; §18.

SPEC §12's gate, quoted exactly, because its precision is the whole point:

> A 30-day backfill: **bronze bytes, normalization, hashing, simhash and entity resolution
> reproduce identically; clustering reproduces within a stated tolerance given a recorded
> ordering key; enrichment resolves from cache with a published hit rate**

That is **three different claims**, and this module's job is to keep them apart. SPEC §18
names over-claiming reproducibility as a known failure mode of projects like this, and the
easy version of this harness — one boolean called `reproducible` — is exactly the over-claim
it warns about.

- **bronze bytes — identical.** The payload is stored, never rewritten. A mismatch here is
  storage corruption, not a code change.
- **normalize, hashing, simhash — identical.** Pure functions of stored bytes
  (`hashing.py`, `transform.py`). No clock, no network, no ordering.
- **entity resolution — identical.** A dictionary lookup at a fixed confidence floor against
  a dictionary snapshot committed to the repo.
- **clustering — within a stated tolerance**, given the recorded ordering key. Greedy
  agglomeration is order-dependent, which is why `cluster.py` records `ordering_key` at all.
  Replaying the key removes the *input* ordering as a variable; what remains is Spark's own
  partitioning of the comparison work, so the honest claim is a high agreement rate rather
  than equality.
- **enrichment — resolves from cache.** The model is not deterministic. Temperature 0 is not
  a guarantee, because GPU kernel scheduling is not bit-stable across runs, so the cache *is*
  the reproducibility mechanism and the published number is a hit rate, not an equality.

## How the comparison works

Each stage re-runs into a **shadow table** under the `repro` namespace and compares against
what the pipeline actually produced. Re-running into the live tables would make the test pass
by overwriting the thing it was meant to check.

Clustering is compared as a **partition**, not by cluster id: agreement is the fraction of
articles whose set of co-members is identical across the two runs. Comparing ids would fail
on a pure relabelling, which is not a reproducibility failure — it is a different name for
the same grouping.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from signal_core.enrich.prompt import PROMPT_VERSION
from signal_core.timeutil import ensure_utc

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Where the shadow tables live. A namespace rather than a suffix so `make clean` and the
# maintenance sweep can both reason about them as a group, and so a shadow table can never be
# mistaken for a real one in a query someone writes at 07:00.
REPRO_NAMESPACE = "repro"

# The clustering agreement floor, **stated rather than fitted**.
#
# 0.95 is a claim that replaying the recorded ordering key reproduces all but a twentieth of
# the grouping. It is set from the argument, not from a measurement that happened to pass:
# `cluster.py` already removes input ordering as a variable, so the residue is Spark's own
# partitioning of the comparison work, which affects only articles sitting near a threshold.
# If a real run lands materially below this, the right response is to find out why rather
# than to lower the number — a tolerance moved to accommodate a result measures nothing.
CLUSTER_AGREEMENT_TOLERANCE = 0.95

# Columns compared for `silver.articles`. Every one is a pure function of stored bytes;
# `fetched_at` and the operational columns are deliberately excluded because they record when
# the pipeline ran, which is not a claim about reproducibility.
ARTICLE_COMPARE_COLUMNS = (
    "article_id",
    "content_hash",
    "simhash",
    "url_canonical",
    "title",
    "published_at",
    "event_date",
    "publisher_domain",
    "story_key",
)


@dataclass(frozen=True)
class StageReport:
    """One claim, checked. `claim` is carried so a reader cannot mistake which is which."""

    stage: str
    claim: str  # "identical" | "tolerance" | "cache"
    compared: int
    matched: int
    tolerance: float | None = None
    notes: str = ""
    examples: tuple[str, ...] = ()

    @property
    def mismatched(self) -> int:
        return self.compared - self.matched

    @property
    def agreement(self) -> float:
        return self.matched / self.compared if self.compared else 1.0

    @property
    def passed(self) -> bool:
        if self.claim == "identical":
            return self.mismatched == 0
        if self.claim == "tolerance":
            return self.agreement >= (self.tolerance or 0.0)
        # "cache" publishes a number rather than gating on one. SPEC §12 asks for a hit rate,
        # not a hit-rate floor, and inventing a floor here would be a claim the spec did not
        # make — §18's over-claiming failure in miniature.
        return True

    def line(self) -> str:
        verdict = "PASS" if self.passed else "FAIL"
        bound = f" (>= {self.tolerance:.3f})" if self.tolerance is not None else ""
        return (
            f"{verdict:4}  {self.stage:22} {self.claim:10} "
            f"{self.matched}/{self.compared} = {self.agreement:.4f}{bound}  {self.notes}"
        )


@dataclass(frozen=True)
class ReproducibilityReport:
    since: datetime
    until: datetime
    stages: list[StageReport] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(stage.passed for stage in self.stages)

    def render(self) -> str:
        days = (self.until - self.since).days
        header = (
            f"Reproducibility over {days} days ({self.since:%Y-%m-%d} to {self.until:%Y-%m-%d})"
        )
        return "\n".join([header, "-" * len(header), *(s.line() for s in self.stages)])


def _shadow(table: str) -> str:
    """`silver.articles` -> `repro.articles`."""
    return f"{REPRO_NAMESPACE}.{table.rsplit('.', 1)[1]}"


def _drop_shadows(spark: SparkSession, tables: tuple[str, ...]) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {REPRO_NAMESPACE}")
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {_shadow(table)} PURGE")


def check_bronze_bytes(spark: SparkSession, since: datetime, until: datetime) -> StageReport:
    """The stored payload still hashes to the hash stored beside it.

    Trivial by design — bronze is never rewritten (SPEC §6.2) — and worth checking anyway,
    because it is the only stage whose failure means storage corruption rather than a code
    change. Everything downstream is a claim about determinism; this one is a claim about the
    bytes still being there.
    """
    from pyspark.sql import functions as F

    from signal_core.hashing import content_hash

    rows = (
        spark.table("bronze.raw_documents")
        .where(
            (F.col("fetched_at") >= F.lit(ensure_utc(since)))
            & (F.col("fetched_at") < F.lit(ensure_utc(until)))
            & (F.col("outcome") == "ok")
        )
        .select("ingest_id", "content_hash", "payload")
        .collect()
    )

    matched = 0
    examples: list[str] = []
    for row in rows:
        if content_hash(bytes(row["payload"])) == row["content_hash"]:
            matched += 1
        elif len(examples) < 3:
            examples.append(row["ingest_id"])

    return StageReport(
        stage="bronze bytes",
        claim="identical",
        compared=len(rows),
        matched=matched,
        notes="stored payload re-hashes to its stored content_hash",
        examples=tuple(examples),
    )


def check_normalize(spark: SparkSession, since: datetime, until: datetime) -> StageReport:
    """Re-parse the same bronze window and compare every derived column.

    This is the stage carrying the most of SPEC §12's "identical" claim: normalization,
    `content_hash` and `simhash64` all happen here, and all three are pure functions of bytes
    that are already on disk.
    """
    from signal_core.spark.jobs.normalize import ARTICLES_TABLE, PARSE_REJECTS_TABLE
    from signal_core.spark.jobs.normalize import normalize_window as run_normalize

    _drop_shadows(spark, (ARTICLES_TABLE, PARSE_REJECTS_TABLE))
    run_normalize(
        spark,
        since,
        until,
        articles_table=_shadow(ARTICLES_TABLE),
        rejects_table=_shadow(PARSE_REJECTS_TABLE),
    )
    return _compare_tables(
        spark,
        stage="normalize + hashing",
        live=ARTICLES_TABLE,
        shadow=_shadow(ARTICLES_TABLE),
        key="article_id",
        columns=ARTICLE_COMPARE_COLUMNS,
        since=since,
        until=until,
        time_column="event_date",
    )


def check_entities(spark: SparkSession, since: datetime, until: datetime) -> StageReport:
    """Re-resolve the same articles against the committed dictionary snapshot.

    Identical rather than tolerant because the resolver is a dictionary lookup at a fixed
    confidence floor (`entities/resolve.py`), scored by `make eval` against a snapshot that
    is committed to the repo. Nothing in it reads a clock or a network.
    """
    from signal_core.spark.jobs.resolve import MENTIONS_TABLE
    from signal_core.spark.jobs.resolve import resolve_window as run_resolve

    _drop_shadows(spark, (MENTIONS_TABLE,))
    run_resolve(spark, since, until, mentions_table=_shadow(MENTIONS_TABLE))
    return _compare_tables(
        spark,
        stage="entity resolution",
        live=MENTIONS_TABLE,
        shadow=_shadow(MENTIONS_TABLE),
        key="mention_id",
        columns=("mention_id", "article_id", "surface_form", "entity_id", "resolution_method"),
        since=since,
        until=until,
        time_column=None,
    )


def _compare_tables(
    spark: SparkSession,
    *,
    stage: str,
    live: str,
    shadow: str,
    key: str,
    columns: tuple[str, ...],
    since: datetime,
    until: datetime,
    time_column: str | None,
) -> StageReport:
    """Row-for-row equality on `columns`, keyed on `key`.

    Compares only rows present in **both**: a row the live table holds from outside this
    window is not evidence about this window, and counting it as a mismatch would make the
    report a function of when it was run.
    """
    from pyspark.sql import functions as F

    available = set(spark.table(live).columns) & set(spark.table(shadow).columns)
    compare = [c for c in columns if c in available]

    live_df = spark.table(live).select(*compare)
    shadow_df = spark.table(shadow).select(*compare)
    if time_column and time_column in available:
        live_df = live_df.where(
            (F.col(time_column) >= F.lit(ensure_utc(since)))
            & (F.col(time_column) < F.lit(ensure_utc(until)))
        )

    joined = live_df.alias("a").join(shadow_df.alias("b"), on=key, how="inner")
    equal = None
    for column in compare:
        if column == key:
            continue
        # `eqNullSafe`, not `==`: `published_at` is nullable by design (SPEC §6.2, ADR-0007)
        # and a null on both sides is agreement, while `NULL = NULL` is null and would
        # silently count every undated article as a mismatch.
        condition = F.col(f"a.{column}").eqNullSafe(F.col(f"b.{column}"))
        equal = condition if equal is None else (equal & condition)

    compared = joined.count()
    matched = joined.where(equal).count() if equal is not None else compared
    examples = (
        tuple(row[key] for row in joined.where(~equal).select(key).limit(3).collect())
        if equal is not None
        else ()
    )

    live_only = spark.table(live).select(key).subtract(spark.table(shadow).select(key)).count()
    return StageReport(
        stage=stage,
        claim="identical",
        compared=compared,
        matched=matched,
        notes=f"{len(compare)} columns; {live_only} live rows absent from the replay",
        examples=examples,
    )


def check_clustering(spark: SparkSession, since: datetime, until: datetime) -> StageReport:
    """Re-cluster the same window and compare the *partition*, not the labels.

    Two runs that group the same articles together but name the groups differently have
    reproduced the clustering. Comparing `cluster_id` would call that a failure, which would
    make the number describe id generation rather than the algorithm.
    """
    from signal_core.spark.jobs.cluster import (
        ARTICLE_CLUSTERS_TABLE,
        CLUSTERS_TABLE,
        cluster_window,
    )

    _drop_shadows(spark, (CLUSTERS_TABLE, ARTICLE_CLUSTERS_TABLE))
    cluster_window(
        spark,
        since,
        until,
        clusters_table=_shadow(CLUSTERS_TABLE),
        map_table=_shadow(ARTICLE_CLUSTERS_TABLE),
    )

    live = _co_members(spark, ARTICLE_CLUSTERS_TABLE)
    replay = _co_members(spark, _shadow(ARTICLE_CLUSTERS_TABLE))

    shared = set(live) & set(replay)
    matched = sum(1 for article in shared if live[article] == replay[article])
    examples = tuple(sorted(a for a in shared if live[a] != replay[a])[:3])

    return StageReport(
        stage="clustering",
        claim="tolerance",
        compared=len(shared),
        matched=matched,
        tolerance=CLUSTER_AGREEMENT_TOLERANCE,
        notes="co-member sets per article, given the recorded ordering key",
        examples=examples,
    )


def _co_members(spark: SparkSession, table: str) -> dict[str, frozenset[str]]:
    """`article_id -> the set of articles it shares a cluster with`."""
    by_cluster: dict[str, set[str]] = {}
    for row in spark.table(table).select("article_id", "cluster_id").collect():
        by_cluster.setdefault(row["cluster_id"], set()).add(row["article_id"])
    return {
        article: frozenset(members - {article})
        for members in by_cluster.values()
        for article in members
    }


def check_enrichment_cache(
    since: datetime, until: datetime, *, client: Any | None = None
) -> StageReport:
    """How much of the window's enrichment resolves from cache without calling the model.

    **This is the only honest claim available here**, and SPEC §12 words it that way on
    purpose. Temperature 0 does not make an LLM bit-reproducible — GPU kernel scheduling is
    not stable across runs — so the cache is what makes a replay produce the same brief, and
    the number to publish is a hit rate.

    Reported, never gated: §12 asks for "a published hit rate", not a floor, and inventing
    one would be exactly the over-claim §18 warns about.
    """
    from signal_core.brief.select import ranked_window
    from signal_core.config import Settings
    from signal_core.enrich.run import ENRICH_TOP_N, read_for_clusters

    settings = Settings()
    hours = max(1, int((ensure_utc(until) - ensure_utc(since)).total_seconds() // 3600))
    window = ranked_window(limit=ENRICH_TOP_N, window_hours=hours, now=until, client=client)
    shown = [c for c in window.clusters if c.get("included")]
    found, _ = read_for_clusters(shown, settings=settings, client=client)

    return StageReport(
        stage="enrichment",
        claim="cache",
        compared=len(shown),
        matched=len(found),
        notes=(
            f"served from cache under {settings.ollama_model_digest[:19]}… / "
            f"{PROMPT_VERSION}; the model is not reproducible, the cache is"
        ),
    )


def verify(
    spark: SparkSession,
    since: datetime,
    until: datetime,
    *,
    include_enrichment: bool = True,
    client: Any | None = None,
) -> ReproducibilityReport:
    """Run every stage and report each claim separately. SPEC §12's 4B acceptance."""
    stages = [
        check_bronze_bytes(spark, since, until),
        check_normalize(spark, since, until),
        check_entities(spark, since, until),
        check_clustering(spark, since, until),
    ]
    if include_enrichment:
        # Optional because it reads through Athena rather than Spark, so a run against a
        # local warehouse with no AWS credentials can still check the four stages that do
        # not need them.
        stages.append(check_enrichment_cache(since, until, client=client))
    return ReproducibilityReport(since=since, until=until, stages=stages)
