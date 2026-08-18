"""The staging landing zone a poller writes to. SPEC §6.1, §6.4, §10.

A poller Lambda hands back bytes; it does not build the lake. So it writes gzipped JSONL
here, and `spark/jobs/commit_bronze.py` — running locally, per SPEC §4's "processing is
local" boundary — converts a staged interval into Parquet and commits it to the
`bronze.raw_documents` Iceberg table.

The split is a packaging decision as much as an architectural one. Writing Parquet in the
Lambda would mean shipping pyarrow (152 MB) plus numpy (33 MB) into a runtime whose zip
ceiling is 250 MB unzipped, for a function whose entire job is one HTTP GET. This module
depends on nothing outside the standard library and boto3, which the Lambda runtime
already provides, so the deployment artifact is the source code plus httpx and pydantic.

Staging is a queue, not a store: `bronze/` is the immutable record (SPEC §6.2), and a
staged object is deletable once committed. Layout matches `storage.bronze_partition`
exactly so the commit job can push down the same partition filters.
"""

from __future__ import annotations

import base64
import gzip
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.timeutil import ingest_partition


def staging_partition(source_id: str, fetched_at: Any) -> str:
    """`source={id}/ingest_date={d}/hour={h}` — identical to the bronze layout (SPEC §6.4).

    Duplicated from `storage.bronze_partition` rather than imported: that module pulls in
    pyarrow at import time, and the whole point of this one is that the Lambda never does.
    """
    ingest_date, hour = ingest_partition(fetched_at)
    return f"source={source_id}/ingest_date={ingest_date}/hour={hour}"


def to_record(doc: RawDocument) -> dict[str, Any]:
    """A JSON-safe dict for one document.

    `payload` is arbitrary bytes — an RSS feed's declared encoding is frequently a lie,
    and EDGAR serves latin-1 in places — so it is base64'd rather than decoded here.
    Decoding is interpretation, and interpretation happens in Spark against stored bytes
    (SPEC §6.1), where getting it wrong is fixable by re-running.
    """
    return {
        "ingest_id": doc.ingest_id,
        "source_id": doc.source_id,
        "fetched_at": doc.fetched_at.isoformat(),
        "source_url": doc.source_url,
        "http_status": doc.http_status,
        "outcome": doc.outcome.value,
        "etag": doc.etag,
        "last_modified": doc.last_modified,
        "content_hash": doc.content_hash,
        "payload_b64": base64.b64encode(doc.payload).decode("ascii"),
        "payload_format": doc.payload_format.value,
        "latency_ms": doc.latency_ms,
        "byte_count": doc.byte_count,
    }


def from_record(record: dict[str, Any]) -> RawDocument:
    """Inverse of `to_record`. Used by tests and by any local replay off staging."""
    from datetime import datetime

    return RawDocument(
        ingest_id=record["ingest_id"],
        source_id=record["source_id"],
        fetched_at=datetime.fromisoformat(record["fetched_at"]),
        source_url=record["source_url"],
        http_status=record["http_status"],
        outcome=FetchOutcome(record["outcome"]),
        etag=record.get("etag"),
        last_modified=record.get("last_modified"),
        content_hash=record["content_hash"],
        payload=base64.b64decode(record["payload_b64"]),
        payload_format=PayloadFormat(record["payload_format"]),
        latency_ms=record["latency_ms"],
        byte_count=record["byte_count"],
    )


def _encode(documents: list[RawDocument]) -> bytes:
    body = "\n".join(json.dumps(to_record(d), sort_keys=True) for d in documents) + "\n"
    # mtime=0 so identical documents produce identical bytes — a replay of the same batch
    # is then byte-comparable, which is what SPEC §12's Phase 4 determinism claim needs.
    return gzip.compress(body.encode("utf-8"), mtime=0)


def write_staging(
    documents: Iterable[RawDocument],
    root: str | Path,
    *,
    client: Any | None = None,
) -> list[str]:
    """Write one gzipped JSONL object per partition. Returns the URIs written.

    `root` is `s3://bucket/prefix` for the deployed pollers or a local path for tests and
    the walking skeleton. Object names carry the batch's first `ingest_id`, so a replay
    lands beside the original instead of overwriting it (SPEC §6.2).
    """
    by_partition: dict[str, list[RawDocument]] = {}
    for doc in documents:
        by_partition.setdefault(staging_partition(doc.source_id, doc.fetched_at), []).append(doc)

    root_str = str(root)
    written: list[str] = []
    for partition, docs in sorted(by_partition.items()):
        name = f"{docs[0].ingest_id}.jsonl.gz"
        body = _encode(docs)
        if root_str.startswith("s3://"):
            bucket, _, prefix = root_str[len("s3://") :].partition("/")
            key = f"{prefix.rstrip('/')}/{partition}/{name}".lstrip("/")
            _s3(client).put_object(Bucket=bucket, Key=key, Body=body)
            written.append(f"s3://{bucket}/{key}")
        else:
            path = Path(root_str) / partition / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            written.append(str(path))
    return written


@dataclass(frozen=True)
class SyncResult:
    """What a staging sync moved, for SPEC §10.3's cost record."""

    downloaded: int
    skipped: int
    bytes_downloaded: int
    local_root: Path

    @property
    def objects(self) -> int:
        return self.downloaded + self.skipped


def sync_staging(
    staging_uri: str,
    dest_root: str | Path,
    *,
    client: Any | None = None,
    source_id: str | None = None,
    ingest_date: str | None = None,
) -> SyncResult:
    """Mirror staged objects to local disk, downloading each one at most once.

    This exists instead of pointing Spark at `s3://` directly, for two reasons and in
    that order of importance:

      1. **Egress is the line item nobody budgets** (SPEC §10.1). Processing runs outside
         AWS, so every read of a staged object is billed internet egress. Letting Spark
         re-read the prefix on each job — and it would, on every retry and every widened
         replay window — is how a $0 month becomes a real bill. Staged objects are
         immutable, so a file already here is already correct: no HEAD, no re-download.
      2. Spark's `s3a://` support means `hadoop-aws` plus a ~500 MB AWS SDK bundle in
         every session, to read files that are a few kilobytes each.

    `source_id` / `ingest_date` narrow the listing to one prefix, so a targeted backfill
    lists one partition rather than the bucket.
    """
    bucket, _, prefix = staging_uri[len("s3://") :].partition("/")
    prefix = prefix.rstrip("/")
    if source_id:
        prefix = f"{prefix}/source={source_id}"
        if ingest_date:
            prefix = f"{prefix}/ingest_date={ingest_date}"

    s3 = _s3(client)
    dest_root = Path(dest_root)
    downloaded = skipped = downloaded_bytes = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl.gz"):
                continue
            local = dest_root / key[len(prefix) :].lstrip("/")
            if local.exists() and local.stat().st_size == obj["Size"]:
                skipped += 1
                continue
            local.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(local))
            downloaded += 1
            downloaded_bytes += obj["Size"]

    return SyncResult(
        downloaded=downloaded,
        skipped=skipped,
        bytes_downloaded=downloaded_bytes,
        local_root=dest_root,
    )


def read_staging(uri: str, *, client: Any | None = None) -> list[RawDocument]:
    """Read one staged object back. The commit job uses Spark for volume; this is for
    tests, local replay, and inspecting a single suspect object by hand."""
    if uri.startswith("s3://"):
        bucket, _, key = uri[len("s3://") :].partition("/")
        body = _s3(client).get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        body = Path(uri).read_bytes()
    lines = gzip.decompress(body).decode("utf-8").splitlines()
    return [from_record(json.loads(line)) for line in lines if line]


def _s3(client: Any | None) -> Any:
    """boto3 is imported lazily so local-only paths (and the walking skeleton) never
    need it, and so `AWS_ENDPOINT_URL` is honoured at call time rather than import time."""
    if client is not None:
        return client
    import boto3

    return boto3.client("s3")
