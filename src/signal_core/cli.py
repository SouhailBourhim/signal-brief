"""Command line entry point."""

from __future__ import annotations

import argparse
import sys


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

    args = parser.parse_args(argv)
    if args.command == "skeleton":
        from signal_core.skeleton import run

        run(use_spark=not args.no_spark, limit=args.limit)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
