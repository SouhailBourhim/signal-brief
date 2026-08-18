"""Bronze -> silver normalize, on Spark. SPEC §7, §9.

Deliberately thin. All domain logic lives in `signal_core.transform`, which has no Spark
import and is unit-tested without a JVM; this module only handles distribution and
schema. When Phase 2 moves the sink to Iceberg, the change is confined here.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from signal_core.transform import normalize_document

if TYPE_CHECKING:  # keeps importing this module cheap for non-Spark callers
    from pyspark.sql import DataFrame, SparkSession

# Order matters: mapInPandas matches the schema positionally.
SILVER_COLUMNS = [
    "article_id",
    "source_id",
    "url_canonical",
    "title",
    "body_text",
    "published_at",
    "fetched_at",
    "lang",
    "publisher_domain",
    "authority_score",
    "simhash",
    "content_hash",
    "timestamp_flagged",
    "story_key",
    "parse_error",
]

SILVER_SCHEMA = """
    article_id string, source_id string, url_canonical string, title string,
    body_text string, published_at timestamp, fetched_at timestamp, lang string,
    publisher_domain string, authority_score double, simhash long,
    content_hash string, timestamp_flagged boolean, story_key string, parse_error string
"""


def build_session(app_name: str = "signal-normalize") -> SparkSession:
    """Local session sized for the dev box.

    `local[*]` rather than a Compose Spark service: identical code, identical `s3a`
    paths, one less moving part. Same argument SPEC §10 makes against EMR (ADR-0002).
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _normalize_partitions(iterator):
    """mapInPandas body: one pandas frame in, one normalized frame out."""
    import pandas as pd

    for pdf in iterator:
        rows = [normalize_document(record) for record in pdf.to_dict("records")]
        yield pd.DataFrame(rows, columns=SILVER_COLUMNS)


def normalize(spark: SparkSession, bronze_root: Path) -> DataFrame:
    """Read bronze parquet, normalize each row, return silver articles.

    Rows that fail to parse are kept with a populated `parse_error` rather than dropped —
    SPEC §6.2 requires quarantine with a reason. The caller splits them into the reject
    table; this job never silently loses a record.
    """
    bronze = spark.read.parquet(str(bronze_root))
    return bronze.mapInPandas(_normalize_partitions, schema=SILVER_SCHEMA)


def split_rejects(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(clean, quarantined) — SPEC §6.2."""
    return silver.filter("parse_error IS NULL"), silver.filter("parse_error IS NOT NULL")
