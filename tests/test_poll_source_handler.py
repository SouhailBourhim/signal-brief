"""The Lambda entry point end to end: DynamoDB state + S3 staging, against local servers.

Uses the `fake` source so no HTTP mocking is needed on top of the AWS-side mocking —
this test is about the handler's plumbing (load state, poll, stage payloads, save state),
not any one source's fetch logic (covered in test_feed_pollers.py / test_hackernews_poller.py).
"""

from __future__ import annotations

import json
import os

import boto3
import pytest
from moto.server import ThreadedMotoServer

from handlers.poll_source import handler as lambda_handler
from signal_core.staging import read_staging
from signal_core.state_store import DynamoDBStateStore

STATE_TABLE = "signal-pipeline-state-test"
BRONZE_BUCKET = "signal-bronze-test"


@pytest.fixture(scope="module")
def moto_endpoint():
    server = ThreadedMotoServer(port=0)
    server.start()
    _, port = server.get_host_and_port()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def aws_env(moto_endpoint, monkeypatch):
    """Both services on the one moto server, never `mock_aws`.

    S3 has to be the real HTTP server because pyarrow uses its own bundled AWS SDK
    (see test_storage_s3.py). DynamoDB could in principle use `mock_aws`, but must not:
    `mock_aws` resets moto's global backends on entry, and the ThreadedMotoServer shares
    those backends in-process — so entering it deletes the bucket created moments before.
    `AWS_ENDPOINT_URL` is a real AWS SDK variable, so the handler's own un-injected
    `boto3.resource("dynamodb")` follows it to the server without any patching.
    """
    monkeypatch.setenv("SOURCE_ID", "fake")
    monkeypatch.setenv("STATE_TABLE_NAME", STATE_TABLE)
    monkeypatch.setenv("BRONZE_STAGING_URI", f"s3://{BRONZE_BUCKET}/staging")
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_endpoint)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    s3 = boto3.client("s3", endpoint_url=moto_endpoint, region_name="us-east-1")
    dynamodb = boto3.client("dynamodb", endpoint_url=moto_endpoint, region_name="us-east-1")
    s3.create_bucket(Bucket=BRONZE_BUCKET)
    dynamodb.create_table(
        TableName=STATE_TABLE,
        KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    yield

    # The server is module-scoped, so each test tears its own state down rather than
    # inheriting the previous one's bronze objects and watermark.
    for obj in s3.list_objects_v2(Bucket=BRONZE_BUCKET).get("Contents", []):
        s3.delete_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])
    s3.delete_bucket(Bucket=BRONZE_BUCKET)
    dynamodb.delete_table(TableName=STATE_TABLE)


def test_stages_payloads_to_s3_and_persists_state(aws_env):
    result = lambda_handler({}, None)

    assert result["source_id"] == "fake"
    assert result["documents"] > 0

    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT_URL"], region_name="us-east-1")
    listing = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="staging/source=fake/")
    assert listing["KeyCount"] == len(result["objects"])

    # The staged bytes are the documents, intact — the commit job has nothing to recover.
    restored = read_staging(result["objects"][0], client=s3)
    assert sum(len(read_staging(uri, client=s3)) for uri in result["objects"]) == result["documents"]
    assert restored[0].payload

    # The whole response has to survive Lambda's JSON serialization of the return value.
    json.dumps(result)

    state = DynamoDBStateStore(STATE_TABLE).load("fake")
    assert state.last_success_at is not None
    assert len(state.seen) == result["documents"]


def test_second_invocation_resumes_from_persisted_state(aws_env):
    first = lambda_handler({}, None)
    second = lambda_handler({}, None)

    assert first["documents"] > 0
    assert second["documents"] == 0  # fake source's fixed fixture is already all `seen`
