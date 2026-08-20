"""Command line entry point."""

from __future__ import annotations

import argparse
import sys

from signal_core.brief.read import CLUSTER_WINDOW_HOURS


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signal")
    sub = parser.add_subparsers(dest="command", required=True)

    skeleton = sub.add_parser("skeleton", help="run the Phase 0 walking skeleton")
    skeleton.add_argument(
        "--no-spark",
        action="store_true",
        help="normalize in-process instead of on Spark (not a toolchain check)",
    )
    skeleton.add_argument("--limit", type=int, default=10, help="stories in the brief")

    brief = sub.add_parser("brief", help="build today's brief from the real lake and render it")
    brief.add_argument("--limit", type=int, default=10, help="stories in the brief")
    brief.add_argument(
        "--window-hours",
        type=int,
        default=CLUSTER_WINDOW_HOURS,
        help="how far back to cluster (SPEC 7.1's same-story window)",
    )
    brief.add_argument("--date", default=None, help="date label, defaults to today in Casablanca")

    athena_query = sub.add_parser(
        "athena-query", help="run a SQL query against the lake, print rows + bytes scanned + cost"
    )
    athena_query.add_argument("--sql", required=True, help="the SQL to run")
    athena_query.add_argument(
        "--database", default=None, help="defaults to settings.athena_database"
    )
    athena_query.add_argument(
        "--workgroup", default=None, help="defaults to settings.athena_workgroup"
    )

    args = parser.parse_args(argv)
    # Aliased on import: both entry points are called `run`, and two lazy `import run`
    # statements in one function body are a redefinition, not a pair of local names.
    if args.command == "brief":
        from signal_core.brief.build import run as build_brief

        build_brief(limit=args.limit, window_hours=args.window_hours, date=args.date)
        return 0
    if args.command == "skeleton":
        from signal_core.skeleton import run as run_skeleton

        run_skeleton(use_spark=not args.no_spark, limit=args.limit)
        return 0
    if args.command == "athena-query":
        from signal_core.cli_athena import run_athena_query

        return run_athena_query(args.sql, database=args.database, workgroup=args.workgroup)
    return 1


if __name__ == "__main__":
    sys.exit(main())
