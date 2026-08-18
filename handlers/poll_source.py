"""AWS Lambda entry point. One artifact, N functions.

SPEC §6.1 asks for one Lambda per source. That is satisfied by deploying this single
handler as N functions — each with its own schedule, IAM role, concurrency limit, and
alarm — parameterized by SOURCE_ID. Terraform `for_each`es the sources map, so adding
source #6 is one module plus one map entry, which is what makes the 30-minute claim real.

Phase 1 fills in the DynamoDB state round-trip and the S3 write; the shape is fixed now
so the pollers written against it do not move later.
"""

from __future__ import annotations

import os
from typing import Any

from signal_core.config import SOURCES
from signal_core.contracts import State
from signal_core.sources import get_poller


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    source_id = os.environ["SOURCE_ID"]
    config = SOURCES[source_id]

    # Phase 1: load from DynamoDB, write back atomically on success.
    state = State(source_id=source_id)

    documents, new_state = get_poller(source_id)(config, state)

    # Phase 1: write_bronze to s3://signal-bronze, persist new_state.
    return {
        "source_id": source_id,
        "documents": len(documents),
        "bytes": sum(d.byte_count for d in documents),
        "outcomes": {
            outcome: sum(1 for d in documents if d.outcome == outcome)
            for outcome in {d.outcome for d in documents}
        },
        "consecutive_failures": new_state.consecutive_failures,
    }
