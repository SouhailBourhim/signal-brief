"""The local half's liveness, published where something else can watch it. SPEC §11.

## Why this exists

SPEC §11's monitoring covers the AWS half. `docs/runbooks/phase-4b.md` records the gap it
leaves: **nothing alerts on a local DAG failing.** Ten hours of dead ingestion produced no
signal at all behind a green AWS console, because every alarm in `monitoring.tf` watches a
Lambda, and the Lambdas were fine — it was the laptop that had stopped.

**The local half fails in two ways and they need two different mechanisms.**

1. **A task fails while the scheduler is alive.** An Airflow `on_failure_callback` sees this.
2. **The scheduler is not alive.** On 2026-08-24 the host was suspended; the containers
   reported `Up` the whole time because they were frozen with it, not stopped. No callback can
   fire here — the process that would run it is the process that is gone. This is the failure
   that needs something *outside* the laptop to notice, which means an alarm on the AWS side
   and therefore a metric the laptop publishes while it is still able to.

A callback alone watches half the failure surface, and the half it does not watch is the half
that actually happened.

## Why a heartbeat rather than an error metric

An alarm on failures can only fire when something ran to fail. Absence is the condition worth
alarming on, and CloudWatch expresses it with `treat_missing_data = "breaching"` — the same
argument `monitoring.tf`'s `poller_silent` alarm already makes for Lambda invocations, applied
to the side of the system that is not in AWS.

## Why two metrics per event

An alarm needs a concrete dimension set; a metric published only as `Dag=cluster` cannot answer
"is *anything* local alive". So each event is published twice: once dimensioned by DAG, for
looking at, and once bare, for alarming on. The bare one is what `local_silent` watches.

Never raises. A monitoring call that can take down the run it monitors has the failure mode
backwards — the callback path already runs inside a task that is failing.
"""

from __future__ import annotations

from typing import Any

import boto3

NAMESPACE = "Signal/Local"

HEARTBEAT_METRIC = "LocalHeartbeat"
FAILURE_METRIC = "LocalFailure"
# The same two events, dimensioned, for reading rather than alarming.
DAG_HEARTBEAT_METRIC = "DagHeartbeat"
DAG_FAILURE_METRIC = "DagFailure"


def _client(client: Any | None = None) -> Any:
    return client if client is not None else boto3.client("cloudwatch")


def _put(metrics: list[dict[str, Any]], client: Any | None) -> bool:
    """Publish, swallowing anything that goes wrong. Returns whether it landed."""
    try:
        _client(client).put_metric_data(Namespace=NAMESPACE, MetricData=metrics)
    except Exception as failure:  # broad on purpose — see the module docstring
        print(f"heartbeat: could not publish to CloudWatch ({failure})")
        return False
    return True


def publish_heartbeat(dag_id: str, *, client: Any | None = None) -> bool:
    """Record that the local scheduler ran `dag_id` to completion just now."""
    return _put(
        [
            {"MetricName": HEARTBEAT_METRIC, "Value": 1, "Unit": "Count"},
            {
                "MetricName": DAG_HEARTBEAT_METRIC,
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [{"Name": "Dag", "Value": dag_id}],
            },
        ],
        client,
    )


def publish_failure(dag_id: str, task_id: str, *, client: Any | None = None) -> bool:
    """Record that a task failed while the scheduler was alive to notice."""
    return _put(
        [
            {"MetricName": FAILURE_METRIC, "Value": 1, "Unit": "Count"},
            {
                "MetricName": DAG_FAILURE_METRIC,
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Dag", "Value": dag_id},
                    {"Name": "Task", "Value": task_id},
                ],
            },
        ],
        client,
    )
