"""The bronze write path against a real S3-compatible HTTP server.

`pyarrow.fs.S3FileSystem` talks to S3 with its own bundled AWS SDK, not botocore, so
`moto`'s usual `mock_aws` decorator (which patches botocore) never sees these requests.
`ThreadedMotoServer` runs an actual local server instead — the same trick used to test
MinIO or any other S3-compatible target — and `storage._resolve_filesystem` is pointed at
it via the real `AWS_ENDPOINT_URL` env var. SPEC §6.4.
"""

from __future__ import annotations

import boto3
import pytest
from moto.server import ThreadedMotoServer

from signal_core.storage import write_bronze

BUCKET = "signal-bronze-test"


@pytest.fixture(scope="module")
def moto_endpoint():
    server = ThreadedMotoServer(port=0)
    server.start()
    _, port = server.get_host_and_port()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def s3_client(moto_endpoint, monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_endpoint)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    client = boto3.client("s3", endpoint_url=moto_endpoint, region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    yield client
    for obj in client.list_objects_v2(Bucket=BUCKET).get("Contents", []):
        client.delete_object(Bucket=BUCKET, Key=obj["Key"])
    client.delete_bucket(Bucket=BUCKET)


def test_write_bronze_writes_to_s3(s3_client, polled):
    documents, _ = polled
    written = write_bronze(documents, f"s3://{BUCKET}/staging")

    assert written
    assert all(str(p).startswith("s3://") for p in written)
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="staging/source=fake/")
    assert listing["KeyCount"] == len(written)


def test_write_bronze_does_not_overwrite_on_replay(s3_client, polled):
    """SPEC §6.2: a replay lands beside the original, never on top of it — same
    guarantee `test_storage.py` asserts locally, now against S3 itself."""
    documents, _ = polled
    first = write_bronze(documents, f"s3://{BUCKET}/staging")
    replayed = [d.model_copy(update={"ingest_id": d.ingest_id + "-replay"}) for d in documents]
    second = write_bronze(replayed, f"s3://{BUCKET}/staging")

    assert set(first).isdisjoint(second)
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="staging/source=fake/")
    assert listing["KeyCount"] == len(first) + len(second)
