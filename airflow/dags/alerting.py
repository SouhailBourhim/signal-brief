"""Shared Airflow callbacks that make the local half visible. SPEC §11; 5.A.

Imported by every DAG in this directory the same way `assets.py` is, and for the same reason:
one definition, so a DAG cannot silently opt out of monitoring by forgetting a keyword.

`signal_core` is imported *inside* the callbacks rather than at module scope. These run in the
scheduler's parse loop, which imports every file in this directory on a short cycle; pulling
boto3 in at parse time would pay for it on every pass, and a parse error here takes out the
whole DAG bag rather than one task.

See `signal_core/ops/heartbeat.py` for why a callback alone is not enough.
"""

from __future__ import annotations

from typing import Any

# Attach to `@dag(default_args=...)` so it reaches every task in the DAG.
DEFAULT_ARGS: dict[str, Any] = {}


def on_task_failure(context: Any) -> None:
    """A task failed while the scheduler was alive. Record it where an alarm can see it."""
    from signal_core.ops.heartbeat import publish_failure

    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    print(f"ALERT: {dag_id}.{task_id} failed")
    publish_failure(dag_id, task_id)


def on_dag_success(context: Any) -> None:
    """A DAG run finished. This is the heartbeat the AWS-side silence alarm watches."""
    from signal_core.ops.heartbeat import publish_heartbeat

    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or "unknown"
    publish_heartbeat(dag_id)


DEFAULT_ARGS["on_failure_callback"] = on_task_failure
