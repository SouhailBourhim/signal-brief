"""Bronze persistence and the read-once object cache. SPEC §6.4, §6.2, §10.1.

The local path convention is identical to the S3 one, so the walking skeleton and the
deployed pipeline differ only in their root. Anything that works against `./data`
works against `s3://signal-bronze` unchanged.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from signal_core.contracts import RawDocument
from signal_core.timeutil import ingest_partition

BRONZE_SCHEMA = pa.schema(
    [
        ("ingest_id", pa.string()),
        ("source_id", pa.string()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("source_url", pa.string()),
        ("http_status", pa.int32()),
        ("outcome", pa.string()),
        ("etag", pa.string()),
        ("last_modified", pa.string()),
        ("content_hash", pa.string()),
        ("payload", pa.binary()),
        ("payload_format", pa.string()),
        ("latency_ms", pa.int32()),
        ("byte_count", pa.int32()),
    ]
)


def bronze_partition(source_id: str, fetched_at: datetime) -> str:
    """`source={id}/ingest_date={d}/hour={h}` — SPEC §6.4, byte-identical to the S3 key."""
    ingest_date, hour = ingest_partition(fetched_at)
    return f"source={source_id}/ingest_date={ingest_date}/hour={hour}"


def write_bronze(documents: Iterable[RawDocument], root: Path) -> list[Path]:
    """Write documents to their partitions. Returns the files written.

    Never overwrites: filenames carry the batch's first ingest_id, so a replay of the
    same interval lands beside the original rather than destroying it (SPEC §6.2).
    """
    by_partition: dict[str, list[RawDocument]] = {}
    for doc in documents:
        by_partition.setdefault(bronze_partition(doc.source_id, doc.fetched_at), []).append(doc)

    written: list[Path] = []
    for partition, docs in sorted(by_partition.items()):
        directory = root / partition
        directory.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(
            [
                {
                    **d.model_dump(mode="python", exclude={"outcome", "payload_format"}),
                    "outcome": d.outcome.value,
                    "payload_format": d.payload_format.value,
                }
                for d in docs
            ],
            schema=BRONZE_SCHEMA,
        )
        path = directory / f"{docs[0].ingest_id}.parquet"
        pq.write_table(table, path, compression="zstd")
        written.append(path)
    return written


class ObjectCache:
    """Content-addressed local cache of bronze objects. SPEC §6.2, §10.1.

    Every bronze object is downloaded at most once. This is a cost control, not a
    convenience: processing runs outside AWS, so each re-read of a bronze object is
    billed internet egress, and a re-run that re-downloads its whole window is the
    failure mode that turns a $0 month into a real bill.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.hits = 0
        self.misses = 0

    def _slot(self, key: str) -> Path:
        # Two-level fan-out: a flat directory of tens of thousands of files is slow to
        # stat on every lookup.
        return self.root / key[:2] / key[2:4] / key

    def get(self, key: str) -> Path | None:
        slot = self._slot(key)
        if slot.exists():
            self.hits += 1
            return slot
        self.misses += 1
        return None

    def put(self, key: str, source: Path) -> Path:
        slot = self._slot(key)
        slot.parent.mkdir(parents=True, exist_ok=True)
        if not slot.exists():
            shutil.copy2(source, slot)
        return slot

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0
