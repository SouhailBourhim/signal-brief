# ADR-0006 — Spark's version is pinned by Iceberg, and pollers never import pyarrow

**Status:** Accepted · **Date:** 2026-08-18

## Context

Two constraints surfaced together while wiring Phase 1's ingest path, and both are about
somebody else's packaging rather than ours.

**Iceberg's Spark runtime is compiled against a specific Spark minor.** The newest
published artifact is `iceberg-spark-runtime-4.1_2.13` (Iceberg 1.11.0); PyPI's newest
pyspark is 4.2.0. The combination is not merely unsupported, it is binary-incompatible —
the first `spark.sql(...)` throws:

```
java.lang.IncompatibleClassChangeError: class org.apache.iceberg.spark.source.SparkView
can not implement org.apache.spark.sql.connector.catalog.View, because it is not an interface
```

A floating `pyspark>=4.0` therefore breaks the lake the next time a machine resolves
dependencies, with an error naming neither Iceberg nor the version constraint.

**Lambda's deployment package has a 250 MB unzipped ceiling.** `write_bronze` produces
Parquet through pyarrow: 152 MB installed, plus numpy at 33 MB. Shipping that into a
function whose entire job is one HTTP GET would put the artifact within ~55 MB of a hard
AWS limit, and pay the unpacking cost on every cold start, for a capability the function
does not need while it is running.

## Decision

- `pyspark>=4.1,<4.2` in both the `spark` extra and the dev group. The upper bound is
  Iceberg's, not ours, and moves the day an Iceberg runtime for the next Spark minor
  ships. `spark/session.py` pins the matching runtime coordinate rather than resolving a
  floating one.
- Poller Lambdas write **gzipped JSONL to a staging prefix** (`signal_core/staging.py`,
  stdlib plus boto3, which the Lambda runtime already provides). A local Spark job
  (`spark/jobs/commit_bronze.py`) converts a staged interval to Parquet and MERGEs it
  into `bronze.raw_documents`. The deployment artifact is source plus httpx and pydantic.

## Consequences

- Spark upgrades are now a deliberate two-line change with a jar coordinate to match, and
  CI will catch the mismatch as a test failure rather than a runtime surprise in Airflow.
- The staging prefix is a second place data can sit, so it needs its own retention: an S3
  lifecycle rule expires staged objects, and `bronze/` remains the immutable record
  (SPEC §6.2). The commit job never deletes staged objects, which is what keeps its own
  retry safe.
- Payloads cross the staging boundary base64-encoded. That is a ~33% size premium on an
  object that lives for days, in exchange for never guessing an encoding — feeds lie
  about theirs, and a poller that decodes wrongly corrupts the only copy (SPEC §6.1).
- A poller can no longer be tested end to end against a Parquet reader; the round-trip
  test in `tests/test_staging.py` covers the boundary instead, and
  `tests/test_commit_bronze.py` covers what Spark makes of it.
