#!/usr/bin/env python3
"""Print unanswered candidates compactly, for labeling. Read-only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

EVALS = Path(__file__).parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("which", choices=("pairs", "mentions"))
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=40)
    parser.add_argument("--body", type=int, default=180, help="body chars to show")
    args = parser.parse_args(argv)

    if args.which == "pairs":
        path = EVALS / "dedup" / "candidates.jsonl"
    else:
        path = EVALS / "entities" / "mentions.jsonl"
    records = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
    if args.which == "mentions":
        records = [r for r in records if "entity_id" not in r]

    window = records[args.start : args.start + args.count]
    print(
        f"# {len(window)} of {len(records)} unanswered ({args.start}..{args.start + len(window)})\n"
    )
    for record in window:
        if args.which == "pairs":
            print(f"## {record['pair_id']}   [{record['stratum']}]")
            for side in ("a", "b"):
                item = record[side]
                body = " ".join(item["body"].split())[: args.body]
                print(f"  {side.upper()} <{item['publisher']}> {item['title']}")
                print(f"     {body}")
            print()
        else:
            print(f"## {record['mention_id']}")
            print(f"  surface: {record['surface_form']!r}")
            print(f"  context: {' '.join(record['context'].split())[:300]}")
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
