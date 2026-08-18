"""Signal — daily tech/finance/economy brief pipeline.

Package is named `signal_core`, not `signal`: `signal` is a stdlib module and the
standard library precedes site-packages on sys.path, so a package by that name is
silently unimportable. PySpark imports stdlib `signal` internally. See ADR-0004.
"""

__version__ = "0.0.0"
