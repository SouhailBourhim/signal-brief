"""Shared Airflow 3 Assets. docs/runbooks/phase-2.md 2.E.

One definition, imported by both the producer (`ingest_monitor_dag.py`) and the
consumer (`process_dag.py`), so a typo in the URI can't silently decouple them —
Airflow matches assets by URI string, not by Python object identity, so two DAGs each
constructing their own `Asset("...")` with the same string would still work, but a
shared constant is what makes a typo a Python error instead of a DAG that quietly never
triggers.
"""

from __future__ import annotations

from airflow.sdk import Asset

# Emitted by `ingest_monitor`'s commit_staged task every time it merges the staged
# interval into bronze.raw_documents — including a replay that merges zero new rows,
# which is still "bronze is now in a state worth normalizing" and correct to trigger on
# (`process_dag`'s window read is idempotent either way; see `normalize_window`'s MERGE).
#
# The URI is a logical key, not a resolvable location: Iceberg's warehouse root is
# environment-dependent (a local Hadoop catalog in tests, s3:// on AWS), so a real path
# would be wrong in one of the two places this Asset is used.
BRONZE_COMMITTED = Asset("iceberg://bronze/raw_documents")

# Emitted by `process`'s normalize_articles. Nothing consumes it on a schedule — `cluster`
# runs on a daily cron instead, because recomputing a 72-hour window on every hourly commit
# is 24x the work for one brief. It is declared anyway so the dependency is visible in
# Airflow's asset graph rather than living only in a cron expression, and so 4A's
# maintenance DAG has something to hang off.
SILVER_COMMITTED = Asset("iceberg://silver/articles")

# Emitted by `cluster`. 4A's brief and mailer are its consumers.
CLUSTERS_COMMITTED = Asset("iceberg://silver/story_clusters")

# Emitted by `resolve`. 3.D's brief joins clusters to entities through it, and SPEC §7.4's
# market-corroboration component is the eventual consumer — it needs the ticker, which is
# what an entity id in the UPPERCASE namespace carries.
MENTIONS_RESOLVED = Asset("iceberg://silver/entity_mentions")

# Emitted by `enrich` (4B). The brief is its consumer, but consumes it as an *inlet* rather
# than a trigger: SPEC §12's acceptance is a 16:00 clock time, so the brief stays on cron and
# renders whatever the cache holds by then. A morning where this never fired produces a brief
# without summaries, which is the degradation `brief/build.py` is built for.
ENRICHMENT_READY = Asset("iceberg://gold/cluster_enrichment")

# Emitted by `macro` (4B). SPEC §8's bitemporal store; the brief reads recent revisions out
# of it. Declared for the same graph-visibility reason as the others — the brief's schedule
# is a clock time, not a dependency.
MACRO_COMMITTED = Asset("iceberg://gold/macro_observations")

# --- The daily chain's own signals -------------------------------------------------
#
# These exist so the once-a-day stages can be ordered by Airflow rather than by cron times
# that have to be read side by side and hoped over. They are deliberately *separate* from
# the assets above: `SILVER_COMMITTED` and `BRONZE_COMMITTED` fire hourly, so scheduling the
# daily stages on those would trigger them ~24x a day — the exact cost each of those DAGs'
# docstrings rejects, and what SPEC §7.3 forbids for enrichment. A daily stage emits a daily
# asset, so the chain runs once per day and in order.
#
# The ordering they encode (market -> macro -> resolve -> cluster -> enrich) is the one the
# cron times already implied (02:30, 02:40, 03:30, 04:00, 05:15 UTC). Nothing about the data
# dependencies changed; what changed is that the order is now enforced instead of assumed,
# because a host that sleeps through the small hours wakes up and fires every overdue cron
# in the same second (see `market_dag`'s gate and `brief_dag`'s docstring).
MARKET_DAILY = Asset("signal://daily/market-loaded")
MACRO_DAILY = Asset("signal://daily/macro-loaded")
