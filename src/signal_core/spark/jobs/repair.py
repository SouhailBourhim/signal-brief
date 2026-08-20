"""Repairing rows that should never have been written. SPEC §6.2, §9, §11.

`silver.articles` MERGEs on `article_id` and is meant to hold one row per article. It holds
132 duplicates, all from articles fetched inside a single hour on 2026-08-19 — the session
where `process` was first unpaused and manually triggered alongside its own schedule
(docs/runbooks/phase-3.md 3.B).

The mechanism is worth stating because it is not obvious: **`MERGE ... WHEN NOT MATCHED THEN
INSERT` is not a uniqueness constraint.** It compiles to an append, Iceberg appends never
conflict with one another, so two writers that both read a pre-insert snapshot both find NOT
MATCHED and both insert. `normalize.ensure_tables` now sets serializable merge isolation so a
second writer fails loudly instead; this job cleans up what landed before that.

**Deleting from the lake needs a reason, and "immutable" is not a reason to keep these.**
A duplicate row is not a second observation of the world — it is one observation recorded
twice by an accounting error. The bytes it came from are still in `bronze.raw_documents`, so
this is reconstructible by replay and destroys no record. That is the test a destructive
maintenance job should have to pass, and it is why the job defaults to a dry run.

    uv run python -c "from signal_core.spark.jobs.repair import ..."   # see the DAG in 4A
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

ARTICLES_TABLE = "silver.articles"


@dataclass(frozen=True)
class RepairResult:
    table: str
    rows_before: int
    rows_after: int
    duplicate_ids: int
    partitions_rewritten: int
    dry_run: bool

    @property
    def rows_removed(self) -> int:
        return self.rows_before - self.rows_after


def find_duplicates(spark: SparkSession, table: str = ARTICLES_TABLE) -> DataFrame:
    """`article_id`s carrying more than one row."""
    from pyspark.sql import functions as F

    return (
        spark.table(table)
        .groupBy("article_id")
        .agg(F.count(F.lit(1)).alias("n"))  # never `count`: Row.count is tuple.count
        .where(F.col("n") > 1)
    )


def repair_duplicates(
    spark: SparkSession, table: str = ARTICLES_TABLE, *, dry_run: bool = True
) -> RepairResult:
    """Collapse duplicate `article_id`s to one row each, atomically per partition.

    Rewrites whole day-partitions rather than deleting rows: `overwritePartitions` replaces
    exactly the partitions present in the DataFrame in one snapshot, so there is no window in
    which the table is missing rows. A DELETE followed by an INSERT would have one, and a
    failure inside it would lose data this job exists to protect.

    Every row of every affected partition is read back, not just the duplicated ids — an
    overwrite that carried only the repaired rows would erase their partition-mates.

    The survivor is the earliest `fetched_at`, which is the first time the pipeline actually
    observed the article. Where the duplicates are byte-identical the choice is immaterial;
    where they differ it is a re-fetch, and the first observation is the one `fetched_at`'s
    "we saw it ourselves" guarantee (SPEC §6.2) is about.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    duplicates = find_duplicates(spark, table)
    duplicate_ids = duplicates.count()
    rows_before = spark.table(table).count()
    if duplicate_ids == 0:
        return RepairResult(table, rows_before, rows_before, 0, 0, dry_run)

    affected = (
        spark.table(table)
        .join(duplicates.select("article_id"), "article_id")
        .select(F.to_date("event_date").alias("day"))
        .distinct()
        .collect()
    )
    days = sorted(row["day"] for row in affected)
    low, high = days[0], days[-1]

    # A closed range over the partition column so Iceberg can prune. `to_date(...) IN (...)`
    # is a transform of the column, not the column, and prunes nothing (ADR-0007's lesson).
    in_range = spark.table(table).where(
        (F.col("event_date") >= F.lit(low)) & (F.col("event_date") < F.date_add(F.lit(high), 1))
    )
    ranked = in_range.withColumn(
        "_rn",
        F.row_number().over(Window.partitionBy("article_id").orderBy(F.col("fetched_at").asc())),
    )
    keep = ranked.where(F.col("_rn") == 1).drop("_rn")

    rows_after = rows_before - (in_range.count() - keep.count())
    partitions = len({row["day"] for row in affected})
    if dry_run:
        return RepairResult(table, rows_before, rows_after, duplicate_ids, partitions, True)

    keep.select(*spark.table(table).columns).writeTo(table).overwritePartitions()
    return RepairResult(
        table, rows_before, spark.table(table).count(), duplicate_ids, partitions, False
    )
