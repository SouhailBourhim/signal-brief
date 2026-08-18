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
