from __future__ import annotations

from datetime import UTC, datetime

import boto3
import pytest
from moto import mock_aws

from signal_core.contracts import State
from signal_core.state_store import DynamoDBStateStore

TABLE_NAME = "signal-pipeline-state-test"


@pytest.fixture
def dynamodb_resource():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{"AttributeName": "source_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield boto3.resource("dynamodb", region_name="us-east-1")


def test_load_missing_source_returns_fresh_state(dynamodb_resource):
    store = DynamoDBStateStore(TABLE_NAME, resource=dynamodb_resource)
    assert store.load("edgar") == State(source_id="edgar")


def test_round_trips_etag_watermark_and_seen(dynamodb_resource):
    store = DynamoDBStateStore(TABLE_NAME, resource=dynamodb_resource)
    now = datetime(2026, 8, 18, 7, 0, tzinfo=UTC)
    state = State(
        source_id="rss_tech",
        etag='"abc123"',
        last_modified="Tue, 18 Aug 2026 07:00:00 GMT",
        watermark=now,
        seen=("a", "b"),
        last_success_at=now,
        consecutive_failures=0,
    )
    store.save(state)

    loaded = store.load("rss_tech")
    assert loaded.etag == state.etag
    assert loaded.last_modified == state.last_modified
    assert loaded.watermark == now
    assert loaded.seen == ("a", "b")
    assert loaded.last_success_at == now
    assert loaded.consecutive_failures == 0


def test_round_trips_an_integer_watermark(dynamodb_resource):
    """Hacker News's watermark is a sequence position, not a timestamp. SPEC §3."""
    store = DynamoDBStateStore(TABLE_NAME, resource=dynamodb_resource)
    store.save(State(source_id="hackernews", watermark=41_000_123))

    loaded = store.load("hackernews")
    assert loaded.watermark == 41_000_123
    assert isinstance(loaded.watermark, int)


def test_failed_poll_persists_failure_count_without_losing_last_good_etag(dynamodb_resource):
    """A save after a failure must not clobber the last good etag/watermark. SPEC §6.2."""
    store = DynamoDBStateStore(TABLE_NAME, resource=dynamodb_resource)
    store.save(State(source_id="edgar", etag='"good"', consecutive_failures=0))

    failed = store.load("edgar").model_copy(update={"consecutive_failures": 1})
    store.save(failed)

    loaded = store.load("edgar")
    assert loaded.etag == '"good"'
    assert loaded.consecutive_failures == 1
