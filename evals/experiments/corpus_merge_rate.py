#!/usr/bin/env python3
"""False-merge rate over random real pairs, per branch. SPEC §7.1, §11.

**The measurement the pairwise eval cannot be.** `evals/score.py` scores 252 labeled pairs;
`group_stories` applies the same decision to ~9.2 million, and union-find takes a transitive
closure, so one false edge merges two components permanently. A per-pair error rate far too
small for 252 pairs to detect is still thousands of edges per window. 3.B found this the
expensive way — pairwise precision 1.000 beside a single cluster holding 59% of the corpus —
and 3.D found it again at a simhash threshold the labeled set scored identically at every
value from 0 to 12.

Both times the measurement was improvised. This is it written down, because 3.E established
that it is not a one-off: of the five constants `fit_thresholds.py` searches, the labeled set
determines **two**. `TITLE_JACCARD` is pinned at 0.35 across every feasible grid point and
`MIN_TITLE_TOKENS` moves the reported held-out number; `NEAR_DUPLICATE_DISTANCE`,
`BODY_JACCARD` and `MIN_BODY_TOKENS` score identically at every value the grid offers. Those
three are decided here or they are decided by a tiebreak, and a tiebreak is not evidence.

## Why an unlabeled sample answers a precision-shaped question

A random pair drawn from a 4,300-article window is same-story with probability near zero —
3.B measured `dedup_ratio` at 1.01, so a few dozen of the 9.2M possible pairs are genuine.
Anything a rule merges here is therefore a false merge to within a rounding error. That is
what makes an unlabeled draw usable, and it is why this measures *only* precision: recall
needs labels, and the labeled set is where recall is measured.

    uv run python evals/experiments/embed_corpus.py dump --out /tmp/articles.json
    uv run python evals/experiments/corpus_merge_rate.py --articles /tmp/articles.json
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from signal_core import dedup  # noqa: E402
from signal_core.hashing import hamming  # noqa: E402

# The three constants the labeled set cannot distinguish, and the shipped value of each.
# Varied one at a time: this answers "what does *this* constant cost", not "what is the best
# joint configuration" — the latter is a fitting question and the labeled set owns it.
SWEEPS = {
    "NEAR_DUPLICATE_DISTANCE": [0, 2, 4, 6, 8, 10, 12],
    "BODY_JACCARD": [0.30, 0.40, 0.50, 0.60],
    "MIN_BODY_TOKENS": [10, 15, 20, 30],
}


def _branch(a: dedup.Prepared, b: dedup.Prepared) -> str | None:
    """Which branch of `decide` fires, or None. Mirrors `decide`'s order exactly.

    Attribution is the point: 3.B's 1,575-article cluster and 3.D's 45-article one were both
    the simhash branch, and a total merge count would have said only that something was
    wrong. Any drift between this and `dedup.decide` makes the attribution a lie, so the
    two are checked against each other on every run — see `_verify`.
    """
    if a.identifiers and b.identifiers and a.identifiers != b.identifiers:
        return None
    a_signal, b_signal = len(a.title) + len(a.body), len(b.title) + len(b.body)
    if (
        a_signal >= dedup.MIN_SIMHASH_TOKENS
        and b_signal >= dedup.MIN_SIMHASH_TOKENS
        and hamming(a.simhash, b.simhash) <= dedup.NEAR_DUPLICATE_DISTANCE
    ):
        return "simhash"
    if (
        len(a.title) >= dedup.MIN_TITLE_TOKENS
        and len(b.title) >= dedup.MIN_TITLE_TOKENS
        and dedup.jaccard(a.title, b.title) >= dedup.TITLE_JACCARD
    ):
        return "title"
    if (
        len(a.body) >= dedup.MIN_BODY_TOKENS
        and len(b.body) >= dedup.MIN_BODY_TOKENS
        and dedup.jaccard(a.body, b.body) >= dedup.BODY_JACCARD
    ):
        return "body"
    return None


def _verify(prepared: list[dedup.Prepared], pairs: list[tuple[int, int]]) -> None:
    """`_branch` must agree with `decide` on every pair, or the attribution is fiction."""
    for i, j in pairs:
        if (_branch(prepared[i], prepared[j]) is not None) != dedup.decide(
            prepared[i], prepared[j]
        ):
            raise AssertionError(f"_branch disagrees with dedup.decide on pair ({i}, {j})")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--articles", type=Path, required=True, help="from `embed_corpus.py dump`")
    parser.add_argument("--n", type=int, default=200_000, help="random pairs to draw")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--show", type=int, default=6, help="example merges per branch")
    args = parser.parse_args(argv)

    articles = json.loads(args.articles.read_text(encoding="utf-8"))
    prepared = [dedup.prepare(a["title"], a["body"]) for a in articles]

    rng = random.Random(args.seed)
    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    while len(pairs) < args.n:
        i, j = rng.randrange(len(articles)), rng.randrange(len(articles))
        if i == j or (min(i, j), max(i, j)) in seen:
            continue
        seen.add((min(i, j), max(i, j)))
        pairs.append((min(i, j), max(i, j)))

    total = len(articles) * (len(articles) - 1) // 2
    print(f"{len(articles):,} articles, {len(pairs):,} random pairs of {total:,}, seed {args.seed}")
    _verify(prepared, pairs)
    print("  `_branch` agrees with `dedup.decide` on every pair")

    shipped = {name: getattr(dedup, name) for name in SWEEPS}
    try:
        for name, values in SWEEPS.items():
            print(f"\n{name}  (shipped: {shipped[name]})")
            for value in values:
                setattr(dedup, name, value)
                counts = {"simhash": 0, "title": 0, "body": 0}
                for i, j in pairs:
                    branch = _branch(prepared[i], prepared[j])
                    if branch:
                        counts[branch] += 1
                merges = sum(counts.values())
                mark = " <- shipped" if value == shipped[name] else ""
                print(
                    f"  {value:<6} {merges:>5} merges = {merges / len(pairs):.4%}  "
                    f"(simhash {counts['simhash']}, title {counts['title']}, "
                    f"body {counts['body']})  ~{total * merges / len(pairs):,.0f} edges/window"
                    f"{mark}"
                )
            setattr(dedup, name, shipped[name])

        print("\nexample merges at the shipped configuration, by branch:")
        shown = {"simhash": 0, "title": 0, "body": 0}
        for i, j in pairs:
            branch = _branch(prepared[i], prepared[j])
            if branch and shown[branch] < args.show:
                shown[branch] += 1
                print(f"  [{branch:<7}] {articles[i]['title'][:58]!r}")
                print(f"            vs {articles[j]['title'][:58]!r}")
    finally:
        for name, value in shipped.items():
            setattr(dedup, name, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
