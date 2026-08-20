#!/usr/bin/env python3
"""Apply labels to a candidate set, recording who made them. SPEC §7.1, §7.2.

Labeling moves a line out of `candidates.jsonl` and into `pairs.jsonl` with the answer
added, so the scorer never reasons about a null and what remains in the candidate file is a
progress bar. Mentions are answered in place, since they have no separate queue.

Every applied label carries a `labeler` field. That is not bookkeeping: `evals/README.md`
stakes the whole harness on the published numbers being trustworthy, and a number computed
against machine-made labels answers a different question than one computed against the
reader's. Recording it in the data is what keeps the distinction from quietly evaporating
between here and the README.

    uv run python evals/label_apply.py pairs         --labels FILE.json --labeler NAME
    uv run python evals/label_apply.py mentions      --labels FILE.json --labeler NAME
    uv run python evals/label_apply.py relabel-pairs --labels FILE.json --labeler NAME
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

EVALS = Path(__file__).parent


def _read(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _write(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records), encoding="utf-8"
    )


def _relabel_pairs(labels: dict[str, Any], labeler: str) -> tuple[int, int]:
    """Overwrite answers already in `pairs.jsonl`, keeping the trail of who changed what.

    This is the review path: a second labeler disagreeing with the first. The new `labeler`
    replaces the old one and `reviewed_from` records what it used to be, so a record always
    says who made the call that stands and who made the one it replaced. Silently
    overwriting would leave the set looking like it had one author all along, which is the
    detail that matters most when the first author was a model.
    """
    path = EVALS / "dedup" / "pairs.jsonl"
    records = _read(path)

    changed = 0
    for record in records:
        if record["pair_id"] not in labels:
            continue
        value = labels[record["pair_id"]]
        if not isinstance(value, bool):
            raise ValueError(f"{record['pair_id']}: same_story must be a bool, got {value!r}")
        if record.get("same_story") == value and record.get("labeler") == labeler:
            continue
        if (previous := record.get("labeler")) and previous != labeler:
            record["reviewed_from"] = previous
        record["same_story"] = value
        record["labeler"] = labeler
        changed += 1

    _write(path, records)
    return changed, sum(1 for r in records if "same_story" not in r)


def _apply_pairs(labels: dict[str, Any], labeler: str) -> tuple[int, int]:
    candidates_path = EVALS / "dedup" / "candidates.jsonl"
    pairs_path = EVALS / "dedup" / "pairs.jsonl"
    candidates = _read(candidates_path)
    pairs = _read(pairs_path)

    answered, remaining = [], []
    for record in candidates:
        if record["pair_id"] in labels:
            value = labels[record["pair_id"]]
            if not isinstance(value, bool):
                raise ValueError(f"{record['pair_id']}: same_story must be a bool, got {value!r}")
            answered.append({**record, "same_story": value, "labeler": labeler})
        else:
            remaining.append(record)

    _write(pairs_path, pairs + answered)
    _write(candidates_path, remaining)
    return len(answered), len(remaining)


def _apply_mentions(labels: dict[str, Any], labeler: str) -> tuple[int, int]:
    """`entity_id` absent means unanswered; `null` means deliberately unlinked, which is a
    correct answer scored as a true negative. A label may be a bare id, or a
    `[entity_id, unlinked_reason]` pair when the answer is "unlinked, and here is why"."""
    path = EVALS / "entities" / "mentions.jsonl"
    records = _read(path)

    answered = 0
    for record in records:
        if record["mention_id"] not in labels:
            continue
        value = labels[record["mention_id"]]
        reason = None
        if isinstance(value, list):
            value, reason = value[0], value[1]
        record["entity_id"] = value
        record["unlinked_reason"] = reason
        record["labeler"] = labeler
        answered += 1

    _write(path, records)
    return answered, sum(1 for r in records if "entity_id" not in r)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("which", choices=("pairs", "mentions", "relabel-pairs"))
    parser.add_argument("--labels", type=Path, required=True, help="JSON {id: answer}")
    parser.add_argument("--labeler", required=True, help="who or what made these judgements")
    args = parser.parse_args(argv)

    labels = json.loads(args.labels.read_text(encoding="utf-8"))
    apply = {
        "pairs": _apply_pairs,
        "relabel-pairs": _relabel_pairs,
        "mentions": _apply_mentions,
    }[args.which]
    applied, remaining = apply(labels, args.labeler)
    print(f"{args.which}: applied {applied}, {remaining} still unanswered")
    if applied != len(labels):
        print(f"  WARNING: {len(labels) - applied} label ids matched nothing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
