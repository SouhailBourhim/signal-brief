"""The staging landing zone: round-trip fidelity and the no-overwrite guarantee.

Staging is the only thing standing between a poller and permanent data loss — nothing
re-fetches a payload once the source has rotated it out (SPEC §6.2) — so what matters
here is that bytes survive the trip exactly and that a replay never lands on top of an
earlier object.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import read_staging, to_record, write_staging
from signal_core.timeutil import utc_now

BUCKET = "signal-bronze-test"


@pytest.fixture
def s3_client():
    """boto3-only, so the in-process `mock_aws` is enough here — unlike the pyarrow write
    path in test_storage_s3.py, which needs a real HTTP server."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _doc(ingest_id: str, payload: bytes) -> RawDocument:
    return RawDocument(
        ingest_id=ingest_id,
        source_id="fake",
        fetched_at=utc_now(),
        source_url="https://example.test/x",
        http_status=200,
        outcome=FetchOutcome.OK,
        etag='W/"abc"',
        last_modified=None,
        content_hash="0" * 64,
        payload=payload,
        payload_format=PayloadFormat.XML,
        latency_ms=12,
        byte_count=len(payload),
    )


def test_round_trip_preserves_bytes_exactly(tmp_path):
    """Payloads are base64'd rather than decoded: feeds lie about their encoding, and a
    poller that guesses wrong corrupts the one copy that exists (SPEC §6.1, §6.2)."""
    # Undeclared latin-1 bytes plus a NUL — both fatal to a naive UTF-8 decode.
    payload = b"\xff\xfe caf\xe9 \x00 <item/>"
    written = write_staging([_doc("fake-001", payload)], tmp_path)

    restored = read_staging(written[0])
    assert len(restored) == 1
    assert restored[0].payload == payload
    assert restored[0].outcome is FetchOutcome.OK
    assert restored[0].etag == 'W/"abc"'


def test_partitions_by_source_and_hour(tmp_path):
    written = write_staging([_doc("fake-001", b"a"), _doc("fake-002", b"b")], tmp_path)

    # One object per partition, not per document: fewer, larger objects is the whole
    # point of a staging batch (SPEC §10.3 — S3 requests are billed per request).
    assert len(written) == 1
    assert "source=fake/ingest_date=" in written[0].replace("\\", "/")
    assert len(read_staging(written[0])) == 2


def test_replay_lands_beside_the_original(tmp_path):
    """SPEC §6.2: raw payloads are immutable and never overwritten."""
    first = write_staging([_doc("fake-001", b"a")], tmp_path)
    second = write_staging([_doc("fake-001-replay", b"a")], tmp_path)

    assert set(first).isdisjoint(second)
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 2


def test_encoding_is_deterministic(tmp_path):
    """Identical documents produce identical bytes — gzip's mtime header is the usual
    culprit, and SPEC §12's Phase 4 determinism claim is measured on stored bytes."""
    doc = _doc("fake-001", b"payload")
    a = write_staging([doc], tmp_path / "a")
    b = write_staging([doc], tmp_path / "b")

    assert Path(a[0]).read_bytes() == Path(b[0]).read_bytes()


def test_writes_to_s3_under_the_prefix(s3_client):
    written = write_staging([_doc("fake-001", b"a")], f"s3://{BUCKET}/staging", client=s3_client)

    assert written[0].startswith(f"s3://{BUCKET}/staging/source=fake/")
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="staging/")
    assert listing["KeyCount"] == 1
    assert read_staging(written[0], client=s3_client)[0].payload == b"a"


def test_staged_object_is_gzipped_jsonl(tmp_path):
    """The commit job reads these with Spark's JSON reader, not this module — so the
    on-disk format is part of the contract, not an implementation detail."""
    written = write_staging([_doc("fake-001", b"a"), _doc("fake-002", b"b")], tmp_path)

    lines = gzip.decompress(Path(written[0]).read_bytes()).decode("utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["ingest_id"] == "fake-001"
    assert set(json.loads(lines[0])) == set(to_record(_doc("x", b"y")))
