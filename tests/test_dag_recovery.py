"""Every DAG can recover on its own from the host suspending mid-run.

ADR-0002 puts this stack on a laptop, and a laptop suspends. The failure that motivated this
file is not the suspension — it is that Airflow could not clean up afterwards. On 2026-09-01
the host froze nine seconds after the scheduler had queued `brief.build` and started
`ingest_monitor.commit_staged`; both mechanisms Airflow ships for reaping the survivors turn
out to be inert under LocalExecutor (`airflow/dags/alerting.py` has the full autopsy), so both
dag_runs stayed `running`, and `max_active_runs=1` meant **neither DAG ever created another
run**. `brief` sat that way through its whole 16:00-23:00 window. Nothing alerted, because the
AWS-side silence alarm asks whether anything local is alive and `maintenance` was still beating.

`dagrun_timeout` is the only lever that works — it fails the run inside the scheduler's own
transaction without consulting the executor — which makes "did someone remember to set it"
exactly the kind of invariant worth a test rather than a convention. Same argument as
`test_compose_paths.py`: a keyword nobody can see missing, guarding a failure nobody sees for
sixteen hours.

Read as AST rather than imported, because Airflow is not a dependency of this project (it runs
in the Compose stack, not the venv) and these files import it at module scope. `alerting.py`
deliberately imports nothing but stdlib, so its constant can be loaded for real.
"""

from __future__ import annotations

import ast
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from signal_core.timeutil import BRIEF_TZ, brief_date

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "airflow" / "dags"
DAG_FILES = sorted(DAGS.glob("*_dag.py"))

# The tightest tick in the directory: `brief` fires hourly across its catch-up window and
# `ingest_monitor` fires hourly all day. A jammed run has to be failed — and its
# `max_active_runs` seat released — before the next slot comes round, or the next slot is
# blocked too and the DAG is stuck permanently rather than for an hour.
TIGHTEST_TICK = timedelta(hours=1)

# The longest DAG run ever recorded on this host: `maintenance`, 907s, taken 2026-09-01 across
# every successful run in Airflow's metadata DB. A timeout that does not clear this by a wide
# margin turns a merely slow day into a failed one.
LONGEST_OBSERVED_RUN = timedelta(seconds=907)

# The three DAGs on a daily cron. They take the longer bound: one of them waits on a sensor for
# hours by design, and a daily tick leaves room the hourly DAGs do not have.
DAILY_DAGS = {"market_dag", "maintenance_dag", "skeleton_dag"}


def _dag_kwargs(path: Path) -> dict[str, ast.expr]:
    """The keyword arguments of the `@dag(...)` decorator in a DAG file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and getattr(decorator.func, "id", "") == "dag":
                return {kw.arg: kw.value for kw in decorator.keywords if kw.arg}
    raise AssertionError(f"{path.name} declares no @dag(...) decorator")


def _timeout(name: str) -> timedelta:
    """Load a timeout constant from `alerting.py` for real — it imports only stdlib."""
    spec = importlib.util.spec_from_file_location("signal_dags_alerting", DAGS / "alerting.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, name)


def _module_constant(path: Path, name: str) -> float:
    """Evaluate a module-level numeric constant without importing the module, which would
    require Airflow. The expression is a literal in this repo, not input."""
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            return eval(compile(ast.Expression(node.value), "<const>", "eval"))
    raise AssertionError(f"{path.name} defines no {name}")


def test_there_are_dags_to_check():
    """A glob that silently matches nothing would make every test below vacuously pass."""
    assert len(DAG_FILES) >= 9, f"only found {len(DAG_FILES)} DAG files in {DAGS}"


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_every_dag_sets_a_dagrun_timeout(path: Path):
    """Without it, one suspend mid-run retires the DAG until a human clears it by hand."""
    kwargs = _dag_kwargs(path)
    assert "dagrun_timeout" in kwargs, (
        f"{path.name} sets no dagrun_timeout; a run interrupted by a host suspend will hold "
        "its max_active_runs seat forever and no later run will ever be created"
    )


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_the_timeout_is_one_of_the_two_shared_ones(path: Path):
    """Named constants, for the same reason `DEFAULT_ARGS` is one definition: a per-DAG literal
    is a per-DAG opportunity to pick a number nobody checked against the tick interval."""
    expected = "DAILY_DAGRUN_TIMEOUT" if path.stem in DAILY_DAGS else "DAGRUN_TIMEOUT"
    node = _dag_kwargs(path).get("dagrun_timeout")
    assert isinstance(node, ast.Name) and node.id == expected, (
        f"{path.name} should take alerting.{expected}"
    )


def test_the_hourly_timeout_clears_a_jam_before_the_next_slot():
    """Longer than the tick and a jam is never cleared in time — the next slot finds the seat
    still taken, and the DAG stays stuck exactly as it did on 2026-09-01."""
    assert _timeout("DAGRUN_TIMEOUT") < TIGHTEST_TICK


def test_neither_timeout_kills_a_legitimately_slow_run():
    assert _timeout("DAGRUN_TIMEOUT") > 2 * LONGEST_OBSERVED_RUN
    assert _timeout("DAILY_DAGRUN_TIMEOUT") > 2 * LONGEST_OBSERVED_RUN


def test_the_daily_timeout_outlasts_the_bronze_gate():
    """`market` opens with a sensor that may wait `GATE_TIMEOUT_SECONDS` for a bronze commit,
    which is exactly what it does after an overnight sleep. A run timeout below that does not
    shorten the wait — it fails it, and `market` heads the daily chain, so it fails the chain.
    Not hypothetical: a 45-minute run timeout did precisely this on 2026-09-01."""
    gate = timedelta(seconds=_module_constant(DAGS / "market_dag.py", "GATE_TIMEOUT_SECONDS"))
    assert _timeout("DAILY_DAGRUN_TIMEOUT") > gate, (
        "the run timeout would cut the bronze gate's wait short and fail the daily chain"
    )


def test_the_last_slot_of_the_brief_window_is_dated_its_own_day():
    """23:00 is the last tick of `brief`'s window, and it belongs to the day it fires on —
    the window is deliberately built not to cross local midnight."""
    assert brief_date(datetime(2026, 8, 31, 23, 0, tzinfo=BRIEF_TZ)) == "2026-08-31"


def test_a_slot_that_executes_after_midnight_is_not_todays_edition():
    """The 2026-09-01 incident, in one assertion. The 23:00 slot of 2026-08-31 did not run
    until 01:15 the next morning, and `due` asked the wall clock what day it was: it answered
    2026-09-01, so the run set about building *today's* brief out of yesterday's data, and
    would have marked today as sent. `due` compares these two values now and skips."""
    slot = datetime(2026, 8, 31, 23, 0, tzinfo=BRIEF_TZ)
    thawed = datetime(2026, 9, 1, 1, 15, tzinfo=BRIEF_TZ)
    assert brief_date(slot) != brief_date(thawed)


def test_due_decides_on_the_runs_own_date_and_not_only_the_clock():
    """A regression here is invisible until the next suspend, and costs a wrong brief plus the
    suppression of the right one."""
    source = (DAGS / "brief_dag.py").read_text(encoding="utf-8")
    due_body = source.split("def due()", 1)[1].split("@task(", 1)[0]
    assert "logical_date" in due_body, "due() no longer consults the run's logical date"
