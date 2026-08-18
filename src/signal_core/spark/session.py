"""Spark session builders. SPEC §4, §5, ADR-0002, ADR-0006.

Two shapes: a plain local session for the walking skeleton, and an Iceberg-enabled one
for anything that touches a table. They are separate because the Iceberg session resolves
a ~40 MB runtime jar from Maven on first use, and the skeleton must stay runnable on a
fresh clone with no network beyond PyPI.

The catalog is `hadoop` locally (a warehouse directory, no service to run) and `glue` on
AWS — SPEC §5 picks the Glue Data Catalog, and Iceberg's own `GlueCatalog` talks to it
directly, so no Glue crawler or Glue ETL job is involved. SPEC §5 excludes those outright.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from signal_core.config import settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Pinned, not floating: a session that silently resolves a different Iceberg build is a
# reproducibility hole, and the Spark minor in the artifact id has to match the installed
# pyspark exactly (ADR-0006 — the pyproject bound exists for this line).
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
ICEBERG_AWS_BUNDLE = "org.apache.iceberg:iceberg-aws-bundle:1.11.0"


def build_session(app_name: str = "signal") -> SparkSession:
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


def build_iceberg_session(
    app_name: str = "signal-iceberg",
    *,
    warehouse: str | Path | None = None,
    catalog: str | None = None,
) -> SparkSession:
    """A session with the Iceberg extensions and one catalog registered.

    `warehouse` decides the backing catalog: an `s3://` URI gets Glue plus S3FileIO (the
    deployed shape), anything else gets a Hadoop catalog over a local directory (tests,
    CI, and local development). Both speak the same SQL, which is what lets the Phase 1
    acceptance test run against a temp directory and still mean something.
    """
    from pyspark.sql import SparkSession

    catalog = catalog or settings.iceberg_catalog
    warehouse = str(warehouse if warehouse is not None else settings.warehouse_uri)
    is_s3 = warehouse.startswith("s3://") or warehouse.startswith("s3a://")

    packages = f"{ICEBERG_RUNTIME},{ICEBERG_AWS_BUNDLE}" if is_s3 else ICEBERG_RUNTIME
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", catalog)
    )
    if is_s3:
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
        ).config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    else:
        builder = builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop")
    return builder.getOrCreate()
