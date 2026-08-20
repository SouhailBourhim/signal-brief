#!/usr/bin/env python3
"""Scoring for the labeled evaluation sets. SPEC §7.1, §7.2, §7.3, §11.

Runs in CI on every PR. An accuracy regression fails the build, which is the mechanism
that makes SPEC's published precision/recall trustworthy rather than a claim made once.

The scorers call the pipeline's own decision functions. They never reimplement the rule —
an eval that scores a reimplementation measures a system nobody ships.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core.dedup import is_same_story
from signal_core.hashing import simhash64

EVALS = Path(__file__).parent


@dataclass
class Score:
    name: str
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0

    @property
    def support(self) -> int:
        return self.tp + self.fp + self.fn + self.tn

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) else 1.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if (self.tp + self.fn) else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    def line(self) -> str:
        return (
            f"{self.name:12} n={self.support:<5} "
            f"precision={self.precision:.3f} recall={self.recall:.3f} f1={self.f1:.3f} "
            f"(tp={self.tp} fp={self.fp} fn={self.fn})"
        )


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


FIXTURE_ORIGIN = "phase0-fixture"


def _score_dedup(name: str, *, fixture: bool) -> Score:
    """Same-story pair classification. SPEC §7.1.

    The two origins are scored separately and gated separately. Folding them into one
    number would let 55 synthetic pairs — correct by construction, since the fixture's
    `story_key` *is* the ground truth — mask roughly a fifth of the real set's failure
    headroom. They answer different questions: the fixture asks "does the harness still
    run", the real set asks "is the clustering any good".
    """
    score = Score(name)
    for pair in _load(EVALS / "dedup" / "pairs.jsonl"):
        if (pair.get("origin") == FIXTURE_ORIGIN) != fixture:
            continue
        a_text = f"{pair['a']['title']} {pair['a']['body']}"
        b_text = f"{pair['b']['title']} {pair['b']['body']}"
        predicted = is_same_story(a_text, b_text, simhash64(a_text), simhash64(b_text))
        actual = pair["same_story"]
        if predicted and actual:
            score.tp += 1
        elif predicted and not actual:
            score.fp += 1
        elif not predicted and actual:
            score.fn += 1
        else:
            score.tn += 1
    return score


def score_dedup() -> Score:
    """The real labeled pairs. This is where the published number comes from."""
    return _score_dedup("dedup", fixture=False)


def score_dedup_fixture() -> Score:
    """The Phase 0 fixture, kept as a canary: it proves the scorer still runs."""
    return _score_dedup("dedup_fixture", fixture=True)


def dedup_by_stratum() -> list[Score]:
    """The real pairs, split by how they were sampled. SPEC §7.1, §11.

    Reporting one combined number for dedup would be misleading in a specific and
    avoidable way. `evals/sample_pairs.py` draws three base-rate-representative strata plus
    one (`focus`) deliberately enriched for the positive class, because same-story pairs are
    rarer than 1-in-60 in this corpus and recall over a base-rate sample would rest on
    almost no positives. Averaging an enriched sample into a representative one produces a
    figure that describes neither.

    So both get published: the representative strata say what the brief's reader actually
    sees, and `focus` says how the rule behaves once a plausible candidate is in front of it.
    """
    pairs = [
        pair
        for pair in _load(EVALS / "dedup" / "pairs.jsonl")
        if pair.get("origin") != FIXTURE_ORIGIN
    ]
    scores: dict[str, Score] = {}
    for pair in pairs:
        stratum = pair.get("stratum", "unsampled")
        score = scores.setdefault(stratum, Score(stratum))
        a_text = f"{pair['a']['title']} {pair['a']['body']}"
        b_text = f"{pair['b']['title']} {pair['b']['body']}"
        predicted = is_same_story(a_text, b_text, simhash64(a_text), simhash64(b_text))
        actual = pair["same_story"]
        if predicted and actual:
            score.tp += 1
        elif predicted and not actual:
            score.fp += 1
        elif not predicted and actual:
            score.fn += 1
        else:
            score.tn += 1
    return [scores[name] for name in sorted(scores)]


def score_entities() -> Score:
    """Mention-to-entity resolution. SPEC §7.2 — labeled set lands in Phase 3."""
    return Score("entities")


def score_enrichment() -> Score:
    """LLM output accuracy against labeled examples. SPEC §7.3 — Phase 4."""
    return Score("enrichment")


SCORERS = {
    "dedup": score_dedup,
    "dedup_fixture": score_dedup_fixture,
    "entities": score_entities,
    "enrichment": score_enrichment,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="score Signal's labeled eval sets")
    parser.add_argument("--gate", action="store_true", help="exit non-zero below threshold")
    parser.add_argument("--only", choices=sorted(SCORERS), help="score one set")
    parser.add_argument(
        "--by-stratum", action="store_true", help="break dedup down by how pairs were sampled"
    )
    args = parser.parse_args(argv)

    thresholds = tomllib.loads((EVALS / "thresholds.toml").read_text(encoding="utf-8"))
    names = [args.only] if args.only else list(SCORERS)

    failed = []
    for name in names:
        score = SCORERS[name]()
        limits = thresholds.get(name, {})
        if score.support == 0:
            print(f"{name:12} no labeled examples yet — not scored")
            continue

        print(score.line())
        for metric in ("precision", "recall"):
            floor = limits.get(f"min_{metric}")
            if floor is not None and getattr(score, metric) < floor:
                failed.append(f"{name}.{metric} {getattr(score, metric):.3f} < {floor}")

    if args.by_stratum:
        print("\ndedup by stratum (`focus` is enriched for positives — not a base rate):")
        for score in dedup_by_stratum():
            print("  " + score.line())

    if failed:
        print("\nFAILED:", *failed, sep="\n  ")
        return 1 if args.gate else 0
    print("\nall gates passed" if args.gate else "\nscored (no gate)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
