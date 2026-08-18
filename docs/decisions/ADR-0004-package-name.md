# ADR-0004 — The package is `signal_core`, not `signal`

**Status:** Accepted · **Date:** 2026-08-18

## Context

The natural package name for a project called Signal is `signal`. It cannot be used:
`signal` is a Python standard library module, and the standard library precedes
site-packages on `sys.path`. A distribution installing a top-level `signal` package would
be silently unimportable — `import signal` keeps resolving to the stdlib.

PySpark imports stdlib `signal` internally to install interrupt handlers, so the failure
would surface as an unrelated crash inside Spark rather than an import error pointing at
the cause.

## Decision

The importable package is `signal_core`. The distribution is `signal-brief`. The CLI
command, the repository, and the product remain `signal`.

## Consequences

- `from signal_core import ...` throughout; the name is slightly less elegant than the
  product name and that is the whole cost.
- The same trap applies to any future module named after a stdlib one (`types`, `json`,
  `logging`, `queue`) — a caution, not a rule, since `src/` layout scopes it to top-level
  names only.
