"""DynamoDB-backed pipeline state. SPEC §6.2, §6.4 architecture diagram.

A Lambda poller has no memory between invocations, so `etag`, `last_modified`,
`watermark`, and the capped `seen` set have to live somewhere that survives it. One item
per source, keyed by `source_id` — small by design, matching `State.SEEN_CAP` (SPEC §6.2:
this is a hot-path item read and written every poll).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

import boto3

from signal_core.contracts import State


class StateStore(Protocol):
    def load(self, source_id: str) -> State: ...
    def save(self, state: State) -> None: ...


def _to_item(state: State) -> dict[str, Any]:
    """Only write attributes that are set. DynamoDB has no concept of a Python `None`,
    and writing one would round-trip back as the string `"None"` rather than absence."""
    item: dict[str, Any] = {
        "source_id": state.source_id,
        "seen": list(state.seen),
        "consecutive_failures": state.consecutive_failures,
    }
    if state.etag is not None:
        item["etag"] = state.etag
    if state.last_modified is not None:
        item["last_modified"] = state.last_modified
    if state.watermark is not None:
        # int (a sequence position, e.g. Hacker News item id) or datetime — tagged so
        # `_from_item` doesn't have to guess which one a stored string represents.
        if isinstance(state.watermark, int):
            item["watermark"] = state.watermark
            item["watermark_kind"] = "int"
        else:
            item["watermark"] = state.watermark.isoformat()
            item["watermark_kind"] = "datetime"
    if state.last_success_at is not None:
        item["last_success_at"] = state.last_success_at.isoformat()
    if state.last_content_change_at is not None:
        item["last_content_change_at"] = state.last_content_change_at.isoformat()
    if state.last_content_hash is not None:
        item["last_content_hash"] = state.last_content_hash
    return item


def _from_item(source_id: str, item: dict[str, Any] | None) -> State:
    """A source with no item yet — first-ever poll — gets fresh state, not an error."""
    if item is None:
        return State(source_id=source_id)

    watermark: datetime | int | None = None
    if item.get("watermark") is not None:
        watermark = (
            int(item["watermark"])
            if item.get("watermark_kind") == "int"
            else datetime.fromisoformat(item["watermark"])
        )

    return State(
        source_id=source_id,
        etag=item.get("etag"),
        last_modified=item.get("last_modified"),
        watermark=watermark,
        seen=tuple(item.get("seen", [])),
        last_success_at=(
            datetime.fromisoformat(item["last_success_at"]) if item.get("last_success_at") else None
        ),
        consecutive_failures=int(item.get("consecutive_failures", 0)),
        last_content_change_at=(
            datetime.fromisoformat(item["last_content_change_at"])
            if item.get("last_content_change_at")
            else None
        ),
        last_content_hash=item.get("last_content_hash"),
    )


class DynamoDBStateStore:
    """SPEC §6.2 persistence for `State`, one item per source_id.

    `resource` is injectable so tests run against `moto` instead of real AWS — the
    Lambda handler leaves it unset and gets the default boto3 resource, which resolves
    credentials and region from the execution environment.
    """

    def __init__(self, table_name: str, *, resource: Any | None = None) -> None:
        self._table = (resource or boto3.resource("dynamodb")).Table(table_name)

    def load(self, source_id: str) -> State:
        response = self._table.get_item(Key={"source_id": source_id})
        return _from_item(source_id, response.get("Item"))

    def save(self, state: State) -> None:
        self._table.put_item(Item=_to_item(state))
