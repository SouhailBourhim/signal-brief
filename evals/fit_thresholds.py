#!/usr/bin/env python3
"""Fit the same-story thresholds against the labeled pairs. SPEC §7.1, §12.

Thresholds are chosen here rather than by eye, and the choice is reproducible: run this and
you get the same numbers, which is the difference between a tuned constant and a lucky one.

**Fit on a train split, report on a held-out one.** Picking thresholds on the same 252 pairs
you then publish precision/recall for is how a portfolio project reports a number it cannot
reproduce on anything else. The split is stratified by label and seeded, so it is stable
across runs; both numbers are printed and both belong in the runbook.

**The objective encodes the asymmetry the labeling rule already states.** From
`evals/dedup/README.md`: a false merge deletes a story from the brief, which the reader never
sees and therefore cannot report; a false split shows a duplicate, which is visible and
cheap. So this maximises recall **subject to** perfect precision, rather than maximising F1 —
F1 would happily trade a deleted story for two recovered ones.

**The Phase 0 fixture is a constraint, not training data.** Its 55 synthetic pairs encode the
capability the real corpus is thinnest on — "Northwind acquires Lumen" against "Lumen to be
bought by Northwind", a genuine rewrite sharing almost no words. Requiring it to keep passing
its own gate is not overfitting to it; it is refusing to buy real-corpus recall by losing the
one case there is barely any real data for.

    uv run python evals/fit_thresholds.py
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import dataclass
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core import dedup

EVALS = Path(__file__).parent
FIXTURE_ORIGIN = "phase0-fixture"

# Named so the grid reads as the thing being decided rather than as five loops.
# `MIN_SIMHASH_TOKENS` is deliberately NOT in this grid, and the reason is the most
# important thing 3.B learned. A labeled set of 252 pairs cannot certify a rule that a
# clustering run applies to 3.6M. Setting it to 0 costs nothing measurable here — every
# candidate scores identically — while over a real window it merged 1.9% of random EDGAR
# pairs, which transitive closure chained into a single 1,575-article cluster holding 59%
# of the corpus. The pairwise objective is blind to that by construction: the damage is a
# property of the closure, not of any pair. So it is set in `dedup.py` from the corpus-level
# measurement, and `group_stories` carries a structural guard for the same reason.
GRID = {
    "NEAR_DUPLICATE_DISTANCE": [0, 2, 4, 6, 8, 10, 12],
    "TITLE_JACCARD": [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50],
    "BODY_JACCARD": [0.30, 0.40, 0.50, 0.60],
    "MIN_TITLE_TOKENS": [2, 3, 4],
    "MIN_BODY_TOKENS": [10, 15, 20, 30],
}


@dataclass(frozen=True)
class Split:
    prepared: list[tuple[dedup.Prepared, dedup.Prepared]]
    actual: list[bool]

    def score(self) -> tuple[float, float, int, int, int]:
        tp = fp = fn = 0
        for (a, b), actual in zip(self.prepared, self.actual, strict=True):
            predicted = dedup.decide(a, b)
            if predicted and actual:
                tp += 1
            elif predicted:
                fp += 1
            elif actual:
                fn += 1
        precision = tp / (tp + fp) if tp + fp else 1.0
        recall = tp / (tp + fn) if tp + fn else 1.0
        return precision, recall, tp, fp, fn


def _load(fixture: bool) -> list[dict]:
    lines = (EVALS / "dedup" / "pairs.jsonl").read_text(encoding="utf-8").splitlines()
    pairs = [json.loads(line) for line in lines if line.strip()]
    return [p for p in pairs if (p.get("origin") == FIXTURE_ORIGIN) == fixture]


def _split(pairs: list[dict], seed: int) -> Split:
    # `prepare` is independent of every threshold in the grid, so it runs once here rather
    # than once per grid point — the difference between seconds and minutes.
    return Split(
        prepared=[
            (
                dedup.prepare(p["a"]["title"], p["a"]["body"]),
                dedup.prepare(p["b"]["title"], p["b"]["body"]),
            )
            for p in pairs
        ],
        actual=[p["same_story"] for p in pairs],
    )


def _stratified_halves(pairs: list[dict], seed: int) -> tuple[list[dict], list[dict]]:
    """Split by label, so a class with 44 members does not land 30/14 by chance."""
    rng = random.Random(seed)
    train: list[dict] = []
    test: list[dict] = []
    for label in (True, False):
        group = [p for p in pairs if p["same_story"] is label]
        rng.shuffle(group)
        half = len(group) // 2
        train.extend(group[:half])
        test.extend(group[half:])
    return train, test


def _fit(train: Split, fixture: Split, min_precision: float) -> dict[str, float] | None:
    """Grid search: maximise recall subject to the precision constraint, with the fixture's
    own gate as a hard filter."""
    best = None
    for values in product(*GRID.values()):
        for name, value in zip(GRID, values, strict=True):
            setattr(dedup, name, value)

        precision, recall, *_ = train.score()
        if precision < min_precision:
            continue
        fixture_precision, fixture_recall, *_ = fixture.score()
        if fixture_precision < 1.0 or fixture_recall < 0.9:
            continue
        # The labeled set does not distinguish simhash distances at all: 0 and 12 score
        # identically on both precision and recall, because once boilerplate is stripped the
        # title path already catches everything stage 2 would. So the tie is broken on
        # documented intent rather than on noise — prefer the distance that keeps SPEC §7.1's
        # stage 2 doing its stated job of catching reprints and light edits, since it costs
        # nothing measurable and covers a case this corpus happens to be too thin to contain
        # (identical prose republished under a different headline). Higher title threshold
        # wins the remaining tie: explicit token agreement over a hash collision.
        key = (recall, values[0], values[1])
        if best is None or key > best[0]:
            best = (key, dict(zip(GRID, values, strict=True)))
    return best[1] if best else None


def _select_constraint(
    pairs: list[dict], fixture: Split, seed: int, candidates: list[float], folds: int = 4
) -> float:
    """Choose the precision constraint by cross-validation **inside train**.

    The constraint is the fitting procedure's own hyperparameter, and picking it by looking
    at the held-out split would spend the split — the reported number would then describe
    data the procedure had already seen. Measured here: constraining train precision to
    exactly 1.0 overfits to that split's particular negatives, while a looser constraint
    finds a point that generalises to perfect precision *and* better recall. That is a real
    effect worth capturing, and capturing it honestly means never showing test data to the
    thing that chooses.
    """
    rng = random.Random(seed)
    shuffled = list(pairs)
    rng.shuffle(shuffled)
    chunks = [shuffled[i::folds] for i in range(folds)]

    measured: dict[float, tuple[float, float]] = {}
    for candidate in candidates:
        precisions, recalls = [], []
        for held in range(folds):
            inner_train = _split([p for i, c in enumerate(chunks) if i != held for p in c], seed)
            inner_test = _split(chunks[held], seed)
            chosen = _fit(inner_train, fixture, candidate)
            if chosen is None:
                break
            for name, value in chosen.items():
                setattr(dedup, name, value)
            precision, recall, *_ = inner_test.score()
            precisions.append(precision)
            recalls.append(recall)
        if len(recalls) != folds:
            continue
        mean_precision = sum(precisions) / folds
        mean_recall = sum(recalls) / folds
        print(
            f"  constraint {candidate:.2f}: cv precision={mean_precision:.3f} "
            f"recall={mean_recall:.3f}"
        )
        measured[candidate] = (mean_precision, mean_recall)

    if not measured:
        return 1.0
    # The selection rule, stated rather than tuned: **never trade precision, but take recall
    # that costs none.** The strictest candidate sets the precision bar; among candidates
    # that hold it, take the best recall. This cannot be reverse-engineered from the answer —
    # it is the README's asymmetry (a false merge deletes a story invisibly, a false split
    # shows a visible duplicate) applied to the selection step rather than only the objective.
    strictest = max(measured)
    bar = measured[strictest][0]
    eligible = {c: r for c, (p, r) in measured.items() if p >= bar}
    return max(eligible, key=lambda c: (eligible[c], c))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--min-precision",
        type=float,
        default=None,
        help="pin the constraint; omit to select it by cross-validation inside train",
    )
    args = parser.parse_args(argv)

    real = _load(fixture=False)
    train_pairs, test_pairs = _stratified_halves(real, args.seed)
    train, test = _split(train_pairs, args.seed), _split(test_pairs, args.seed)
    fixture = _split(_load(fixture=True), args.seed)
    print(
        f"real pairs {len(real)} -> train {len(train_pairs)} "
        f"({sum(p['same_story'] for p in train_pairs)} positive) / "
        f"test {len(test_pairs)} ({sum(p['same_story'] for p in test_pairs)} positive); "
        f"fixture {len(fixture.actual)}"
    )

    original = {name: getattr(dedup, name) for name in GRID}
    try:
        if args.min_precision is None:
            print("\nselecting the precision constraint by 4-fold CV inside train:")
            constraint = _select_constraint(
                train_pairs, fixture, args.seed, [1.0, 0.95, 0.90, 0.85, 0.80]
            )
            print(f"  -> {constraint:.2f}")
        else:
            constraint = args.min_precision

        chosen = _fit(train, fixture, constraint)
        if chosen is None:
            print(f"\nno grid point reaches precision {constraint} — loosen it")
            return 1

        for name, value in chosen.items():
            setattr(dedup, name, value)
        print("\nchosen:")
        for name, value in chosen.items():
            print(f"  {name:<24} {value}")
        print(f"  {'MIN_SIMHASH_TOKENS':<24} {dedup.MIN_SIMHASH_TOKENS}  (held fixed, not fitted)")
        print()
        for label, split in (
            ("train (fitted on)", train),
            ("HELD OUT", test),
            ("fixture", fixture),
        ):
            precision, recall, tp, fp, fn = split.score()
            print(
                f"  {label:<18} precision={precision:.3f} recall={recall:.3f} "
                f"(tp={tp} fp={fp} fn={fn})"
            )
    finally:
        for name, value in original.items():
            setattr(dedup, name, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
