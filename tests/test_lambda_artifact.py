"""What the poller Lambda is allowed to import. ADR-0006.

The deployment artifact contains httpx, pydantic, pydantic-settings, and the source tree
— nothing else, because pyarrow alone is 152 MB against a 250 MB unzipped ceiling. That
constraint is invisible in the source: any `from signal_core.storage import ...` added to
a module the handler happens to touch would break the deployed function on import, and
only there. So it is asserted here instead, where it fails on a laptop in a second.
"""

from __future__ import annotations

import subprocess
import sys

# Heavy, and every one of them is a dependency the handler's chain must not acquire.
FORBIDDEN = ("pyarrow", "pyspark", "pandas", "numpy", "jinja2")


def test_handler_import_chain_stays_light():
    probe = (
        f"import handlers.poll_source, sys; print([m for m in {FORBIDDEN!r} if m in sys.modules])"
    )
    # A subprocess, not an assertion on this process's sys.modules: pytest has already
    # imported half the project by the time this runs.
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]", f"handler pulled in {result.stdout.strip()}"


def test_the_watchlist_ships_beside_the_code_that_reads_it():
    """4A.D's market poller resolves its ticker list from `watchlist.toml` **at fetch time**,
    so the file is a runtime dependency of the deployed function, not just a dev convenience.

    Data files are exactly what a packaging step forgets: the code imports fine, the tests
    pass, and the Lambda raises `FileNotFoundError` on its first real invocation — which is
    the same "breaks only there" failure class this module was written for.
    """
    from signal_core.watchlist import WATCHLIST_PATH

    assert WATCHLIST_PATH.exists()
    # Inside the package directory, which is what `cp -r src/signal_core` and hatchling's
    # `packages = ["src/signal_core"]` both carry. A watchlist parked at the repo root would
    # satisfy every test and ship in neither.
    assert WATCHLIST_PATH.parent.name == "signal_core"


def test_the_poller_can_resolve_its_tickers_without_the_heavy_stack():
    """The market poller's whole reason for existing over yfinance (ADR-0010). Reading the
    watchlist must not be what drags pandas back in."""
    probe = (
        "import sys; from signal_core.sources.market import poll; "
        "from signal_core.watchlist import load; assert load().tickers(); "
        f"print([m for m in {FORBIDDEN!r} if m in sys.modules])"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]", f"market poller pulled in {result.stdout.strip()}"
