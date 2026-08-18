"""Bronze persistence and the read-once object cache. SPEC §6.4, §6.2, §10.1.

The local path convention is identical to the S3 one, so the walking skeleton and the
deployed pipeline differ only in their root. Anything that works against `./data`
works against `s3://signal-bronze` unchanged.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pa_fs

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


def _resolve_filesystem(root: Path | str) -> tuple[pa_fs.FileSystem, str]:
    """(filesystem, base_path) for `root` — `s3://...` or a local path.

    The local walking skeleton and the deployed Lambda pollers share `write_bronze`
    unchanged; only the root differs. Keeping the filesystem choice here, rather than in
    every caller, is what makes that true.
    """
    root_str = str(root)
    if root_str.startswith("s3://"):
        # AWS_ENDPOINT_URL (a real AWS SDK env var) redirects to a local S3-compatible
        # server — moto's ThreadedMotoServer in tests, or MinIO for local dev — instead
        # of real S3. Unset, this talks to AWS using the Lambda execution role.
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        s3_kwargs = {}
        if endpoint:
            scheme, _, host = endpoint.partition("://")
            s3_kwargs = {"endpoint_override": host, "scheme": scheme}
        return pa_fs.S3FileSystem(**s3_kwargs), root_str[len("s3://") :]
    # pyarrow's local filesystem expects forward slashes even on Windows (e.g. "C:/x"),
    # and we join partitions with "/" below regardless of platform.
    return pa_fs.LocalFileSystem(), root_str.replace("\\", "/")


def write_bronze(documents: Iterable[RawDocument], root: Path | str) -> list[Path | str]:
    """Write documents to their partitions. Returns the files written.

    Never overwrites: filenames carry the batch's first ingest_id, so a replay of the
    same interval lands beside the original rather than destroying it (SPEC §6.2).

    `root` is a local path for the walking skeleton or an `s3://bucket/prefix` URI for
    the deployed pollers (SPEC §6.4) — the write path is identical either way.
    """
    by_partition: dict[str, list[RawDocument]] = {}
    for doc in documents:
        by_partition.setdefault(bronze_partition(doc.source_id, doc.fetched_at), []).append(doc)

    is_s3 = str(root).startswith("s3://")
    filesystem, base = _resolve_filesystem(root)
    base = base.rstrip("/")

    written: list[Path | str] = []
    for partition, docs in sorted(by_partition.items()):
        directory = f"{base}/{partition}"
        if not is_s3:
            # S3 has no real directories — object keys with slashes are the layout —
            # and pre-creating one there would write spurious zero-byte marker objects
            # into every partition on every poll (SPEC §10.3: S3 requests are billed).
            filesystem.create_dir(directory, recursive=True)
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
        object_path = f"{directory}/{docs[0].ingest_id}.parquet"
        with filesystem.open_output_stream(object_path) as sink:
            pq.write_table(table, sink, compression="zstd")
        written.append(f"s3://{object_path}" if is_s3 else Path(object_path))
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
