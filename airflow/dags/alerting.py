"""Shared Airflow policy that makes the local half visible — and survivable. SPEC §11; 5.A.

Imported by every DAG in this directory the same way `assets.py` is, and for the same reason:
one definition, so a DAG cannot silently opt out of monitoring by forgetting a keyword.

`signal_core` is imported *inside* the callbacks rather than at module scope. These run in the
scheduler's parse loop, which imports every file in this directory on a short cycle; pulling
boto3 in at parse time would pay for it on every pass, and a parse error here takes out the
whole DAG bag rather than one task.

See `signal_core/ops/heartbeat.py` for why a callback alone is not enough.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

# Attach to `@dag(default_args=...)` so it reaches every task in the DAG.
DEFAULT_ARGS: dict[str, Any] = {}

# Attach to `@dag(dagrun_timeout=...)`, which `default_args` cannot carry — it is a DAG-level
# argument, not a task-level one, so every DAG here has to pass it by hand.
#
# **The failure this exists for has no other exit.** ADR-0002 runs this stack on a laptop, and
# a laptop suspends mid-run. On 2026-09-01 the host froze nine seconds after the scheduler had
# queued `brief.build` and started `ingest_monitor.commit_staged`. Neither task survived the
# thaw, and — the part worth writing down — neither could be reaped, because *both* mechanisms
# Airflow ships for this are inert under LocalExecutor:
#
#   - **Stuck in `queued`** (`brief.build`). `_handle_tasks_stuck_in_queued` reaches for
#     `executor.revoke_task()`, which `LocalExecutor` does not implement; `BaseExecutor` raises
#     `NotImplementedError`, the scheduler catches it and moves on to the next executor. The
#     tell is that sixteen hours in `queued` produced zero `stuck in queued` rows in `log`.
#   - **Stuck in `running`** (`ingest_monitor.commit_staged`). The heartbeat reaper does fire,
#     but it only asks the executor to fail the task; `_process_executor_events` reaches
#     `ti.handle_failure()` — the call that actually writes FAILED to the database — solely on
#     the branch where the task has **no** `on_failure_callback`. `DEFAULT_ARGS` above puts one
#     on every task in this directory, so it takes the other branch, sends a callback, and
#     leaves the state `running`. The reaper finds the same task ten seconds later, forever:
#     204 `heartbeat timeout` rows had accumulated before anyone looked.
#
# Both leave the dag_run itself `running`, and `max_active_runs=1` — which every DAG here sets,
# each for its own good reason — then means **no further run of that DAG is ever created**.
# Not a lost day: a lost DAG, until a human clears it. `brief` sat like that through its entire
# 16:00-23:00 window while the AWS-side silence alarm read OK, because `maintenance` was still
# beating and that alarm asks whether *anything* local is alive, not whether everything is.
#
# `dagrun_timeout` is the one lever that works, precisely because it touches neither the
# executor nor the callback path: the scheduler fails the run inside its own transaction, sets
# every unfinished task to SKIPPED, and — the part that actually unblocks the DAG —
# recalculates `next_dagrun`, which a jammed run leaves pinned to the slot it died in.
#
# **Two values, because the bound is genuinely different at the two schedules, and picking one
# number for both breaks something.** A timeout has to sit above everything the DAG may
# legitimately spend — retries and sensor waits included, not just the runs that went well —
# and below its own tick, or a jam is never cleared before the next slot needs the seat.
#
# For an hourly DAG the ceiling is the binding constraint: 45 minutes is the most that still
# clears inside `brief`'s and `ingest_monitor`'s hour. It fits what they actually do — 13
# minutes is the longest `ingest_monitor` ever ran, and `brief`'s slowest build plus both of
# `mail`'s five-minute retries is about 20.
#
# For a daily DAG the *floor* binds instead, and 45 minutes sits below it. `market` opens with
# a sensor that is designed to wait up to `GATE_TIMEOUT_SECONDS` — four hours — for bronze to
# be committed, which is precisely the situation after an overnight sleep; 45 minutes truncates
# that wait and converts it into a failed run, and `market` heads the daily chain, so it
# converts it into a failed *chain*. `maintenance` is less dramatic and the same shape: 907s
# plus a five-minute retry_delay plus another 907s is 35 minutes, which is not a margin.
# Six hours clears both by a wide margin and is still a quarter of a daily tick.
DAGRUN_TIMEOUT = timedelta(minutes=45)
DAILY_DAGRUN_TIMEOUT = timedelta(hours=6)


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
