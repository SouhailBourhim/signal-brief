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

    feedback = sub.add_parser(
        "feedback", help="mark a story in a brief up or down (SPEC 7.4's feedback component)"
    )
    feedback.add_argument(
        "cluster_id",
        nargs="?",
        default=None,
        help="the story's cluster id; omit with --list to see them",
    )
    feedback.add_argument(
        "mark",
        nargs="?",
        default=None,
        choices=("up", "down", "clear"),
        help="up, down, or clear to remove a mark",
    )
    feedback.add_argument("--date", default=None, help="which brief, defaults to today")
    feedback.add_argument(
        "--list",
        action="store_true",
        dest="list_items",
        help="show what that brief contained, with any marks already left",
    )

    cost = sub.add_parser(
        "cost", help="what this project has cost, by the `project` cost-allocation tag"
    )
    cost.add_argument("--days", type=int, default=30, help="how far back, default 30")
    cost.add_argument("--project", default="signal", help="the tag value to filter on")

    enrich = sub.add_parser(
        "enrich", help="run the Ollama enrichment stage over the ranked head of the window"
    )
    enrich.add_argument(
        "--limit", type=int, default=None, help="how many heads, default enrich.run.ENRICH_TOP_N"
    )
    enrich.add_argument(
        "--check-model",
        action="store_true",
        help="report the local model digest against the pin, and exit without inferring",
    )

    reproduce = sub.add_parser(
        "reproduce", help="SPEC §12's 4B acceptance: replay a window and report each claim"
    )
    reproduce.add_argument("--days", type=int, default=30, help="window length, default 30")
    reproduce.add_argument(
        "--no-enrichment",
        action="store_true",
        help="skip the cache stage, which reads through Athena and needs credentials",
    )

    sub.add_parser(
        "streak",
        help="consecutive daily briefs, from gold.brief_items (SPEC 16.5)",
    )

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
    if args.command == "feedback":
        from signal_core.cli_feedback import run_feedback, run_list

        if args.list_items:
            return run_list(args.date)
        if not args.cluster_id or not args.mark:
            # argparse cannot express "these two are required unless --list", and a missing
            # mark must not be read as "clear" — that would silently erase a mark the reader
            # meant to keep.
            parser.error("feedback needs a cluster_id and a mark, or --list")
        return run_feedback(args.date, args.cluster_id, args.mark)
    if args.command == "cost":
        from datetime import date, timedelta

        from signal_core.ops.costs import format_cost, project_cost

        end = date.today()
        print(format_cost(project_cost(end - timedelta(days=args.days), end, project=args.project)))
        return 0
    if args.command == "enrich":
        from signal_core.cli_enrich import run_check_model, run_enrich

        if args.check_model:
            return run_check_model()
        return run_enrich(limit=args.limit)
    if args.command == "reproduce":
        from datetime import timedelta

        from signal_core.ops.reproduce import verify
        from signal_core.spark.session import build_iceberg_session
        from signal_core.timeutil import utc_now

        until = utc_now()
        spark = build_iceberg_session("signal-reproduce")
        try:
            report = verify(
                spark,
                until - timedelta(days=args.days),
                until,
                include_enrichment=not args.no_enrichment,
            )
        finally:
            spark.stop()
        print(report.render())
        # Non-zero on failure so this can gate. The enrichment stage never fails — it
        # publishes a rate rather than gating on one (SPEC §12 asks for the rate).
        return 0 if report.passed else 1
    if args.command == "streak":
        from signal_core.brief.read import read_brief_streak

        streak, query = read_brief_streak()
        print(f"{streak.describe()}  ({streak.total_briefs} briefs, longest {streak.longest})")
        if streak.first_date:
            print(f"  first {streak.first_date} · current run from {streak.started}")
        if streak.missing:
            # Named, not counted: SPEC 17's rule against inventing a metric applies just as
            # much to hiding one, and "3 missed" is the shape of a number nobody chases.
            print(f"  missed: {', '.join(str(d) for d in streak.missing)}")
        print(f"  {query.bytes_scanned:,} bytes scanned, ${query.cost_usd:.6f}")
        return 0

    if args.command == "athena-query":
        from signal_core.cli_athena import run_athena_query

        return run_athena_query(args.sql, database=args.database, workgroup=args.workgroup)
    return 1


if __name__ == "__main__":
    sys.exit(main())
