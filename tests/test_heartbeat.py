"""The local half's liveness metrics. SPEC §11; docs/runbooks/phase-5.md 5.A."""

from __future__ import annotations

from typing import Any

from signal_core.ops import heartbeat


class FakeCloudWatch:
    def __init__(self, fails: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fails = fails

    def put_metric_data(self, **kwargs: Any) -> None:
        if self.fails:
            raise RuntimeError("no credentials")
        self.calls.append(kwargs)


def _names(client: FakeCloudWatch) -> set[str]:
    return {m["MetricName"] for call in client.calls for m in call["MetricData"]}


def test_a_heartbeat_publishes_both_the_bare_and_the_dimensioned_metric():
    """An alarm needs a concrete dimension set, so 'is anything alive' needs a bare metric.

    Publishing only `DagHeartbeat{Dag=cluster}` would mean the silence alarm had to name one
    DAG and would go quiet the day that DAG is renamed.
    """
    client = FakeCloudWatch()
    assert heartbeat.publish_heartbeat("cluster", client=client) is True
    assert _names(client) == {heartbeat.HEARTBEAT_METRIC, heartbeat.DAG_HEARTBEAT_METRIC}
    assert client.calls[0]["Namespace"] == heartbeat.NAMESPACE

    dimensioned = [m for m in client.calls[0]["MetricData"] if m.get("Dimensions")]
    assert dimensioned[0]["Dimensions"] == [{"Name": "Dag", "Value": "cluster"}]


def test_a_failure_carries_the_dag_and_the_task():
    client = FakeCloudWatch()
    assert heartbeat.publish_failure("brief", "mail", client=client) is True
    assert _names(client) == {heartbeat.FAILURE_METRIC, heartbeat.DAG_FAILURE_METRIC}

    dimensioned = [m for m in client.calls[0]["MetricData"] if m.get("Dimensions")]
    assert dimensioned[0]["Dimensions"] == [
        {"Name": "Dag", "Value": "brief"},
        {"Name": "Task", "Value": "mail"},
    ]


def test_a_broken_cloudwatch_never_takes_down_the_run_it_is_watching(capsys):
    """The failure callback runs inside a task that is already failing.

    A monitoring call that raises there would replace the real error with its own, which is
    the failure mode backwards.
    """
    client = FakeCloudWatch(fails=True)
    assert heartbeat.publish_failure("brief", "mail", client=client) is False
    assert heartbeat.publish_heartbeat("brief", client=client) is False
    assert "could not publish" in capsys.readouterr().out
