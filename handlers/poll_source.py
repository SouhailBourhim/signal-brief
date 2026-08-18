"""AWS Lambda entry point. One artifact, N functions.

SPEC §6.1 asks for one Lambda per source. That is satisfied by deploying this single
handler as N functions — each with its own schedule, IAM role, concurrency limit, and
alarm — parameterized by SOURCE_ID. Terraform `for_each`es the sources map, so adding
source #6 is one module plus one map entry, which is what makes the 30-minute claim real.

This handler's job stops at "fetch and hand back bytes, persist state" (SPEC §6.1):
payloads land as gzipped JSONL in a staging prefix here, and a local Spark job — SPEC
§4's "processing is local" boundary — converts them to Parquet and commits them into the
`bronze.raw_documents` Iceberg table on its own schedule
(`signal_core/spark/jobs/commit_bronze.py`). Keeping Parquet out of the Lambda is also
what keeps the deployment artifact small enough to zip; see `signal_core/staging.py`.
Every poller in the registry
already returns failures as `outcome=ERROR` documents rather than raising (SPEC §6.2:
quarantined, never silently dropped), so an exception escaping this function means a real
infrastructure problem — DynamoDB, S3, a bug — and should crash the invocation so
CloudWatch's Lambda-errors alarm fires as the backstop signal.
"""

from __future__ import annotations

import os
from typing import Any

from signal_core.config import SOURCES, settings
from signal_core.sources import get_poller
from signal_core.staging import write_staging
from signal_core.state_store import DynamoDBStateStore


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    source_id = os.environ["SOURCE_ID"]
    config = SOURCES[source_id]
    store = DynamoDBStateStore(os.environ.get("STATE_TABLE_NAME", settings.state_table_name))

    state = store.load(source_id)
    documents, new_state = get_poller(source_id)(config, state)

    # Bronze first, state second. If the save fails after a successful write the next
    # invocation re-fetches and re-stages an overlapping interval, which dedup collapses
    # (SPEC §7.1). The other order loses documents outright, and SPEC §6.2 says raw
    # payloads are never re-fetched — so duplicates are the survivable failure.
    staged = write_staging(
        documents, os.environ.get("BRONZE_STAGING_URI", settings.bronze_staging_uri)
    )
    store.save(new_state)

    return {
        "source_id": source_id,
        "documents": len(documents),
        "objects": staged,
        "bytes": sum(d.byte_count for d in documents),
        # `.value`, not the enum: this dict is serialized into the invocation response
        # and into the CloudWatch log line Airflow's monitoring DAG reads (SPEC §11).
        "outcomes": {
            outcome.value: sum(1 for d in documents if d.outcome == outcome)
            for outcome in {d.outcome for d in documents}
        },
        "consecutive_failures": new_state.consecutive_failures,
    }
