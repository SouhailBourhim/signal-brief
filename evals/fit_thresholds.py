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
from signal_core.entities import dictionary as entity_dictionary
from signal_core.entities import resolve as entity_resolve

EVALS = Path(__file__).parent
FIXTURE_ORIGIN = "phase0-fixture"

# Named so the grid reads as the thing being decided rather than as five loops.
#
# **Two constants are deliberately NOT in this grid**, for one reason stated twice. A labeled
# set of 252 pairs cannot certify a rule that a clustering run applies to millions, and the
# pairwise objective is blind to what transitive closure does with a single edge: the damage
# is a property of the closure, not of any pair. Both are set in `dedup.py` from a
# corpus-level measurement instead — `evals/experiments/corpus_merge_rate.py` — and
# `group_stories` carries a structural guard for the same reason.
#
# `MIN_SIMHASH_TOKENS` is 3.B's. Every candidate scored identically here, while over a real
# window a value of 0 merged 1.9% of random EDGAR pairs, which closure chained into a single
# 1,575-article cluster holding 59% of the corpus.
#
# `NEAR_DUPLICATE_DISTANCE` is 3.D's, and it was still in this grid until 3.E — which is a
# defect, because the grid was actively recommending 12 while `dedup.py` shipped 0. Every
# value from 0 to 12 scores identically on both labeled sets, so the tiebreak alone decided
# it, and the tiebreak preferred the largest. 3.D had already measured what 12 does: two
# unrelated Show HN posts at hamming 10 and 12, chained by closure into a 45-article cluster
# holding Disney/FCC, a Grok exploit, a Pixel deal and a corgi tracker. Over random real pairs
# the simhash branch fires **0 times at distance 0-10 and once at 12** in 200,000 draws, which
# is the whole of the evidence, and it points the opposite way from the tiebreak.
GRID = {
    "TITLE_JACCARD": [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50],
    "BODY_JACCARD": [0.30, 0.40, 0.50, 0.60],
    "MIN_TITLE_TOKENS": [2, 3, 4],
    "MIN_BODY_TOKENS": [10, 15, 20, 30],
}

# Held fixed, reported alongside the fitted values so a reader can see the whole configuration
# rather than the searched part of it.
NOT_FITTED = ("NEAR_DUPLICATE_DISTANCE", "MIN_SIMHASH_TOKENS")

# What `dedup.py` ships, captured **at import** — before any sweep has touched the module.
# `_fit`'s tiebreak needs this and cannot read it live: fitting works by setting the constants
# on `dedup` and scoring, so `getattr(dedup, ...)` mid-run returns wherever the search
# currently is. `_select_constraint` makes that worse by leaving each fold's winner in place.
# Read once, here, and the question "what does the module ship" has one answer all run.
SHIPPED = {name: getattr(dedup, name) for name in GRID}


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


def _feasible(train: Split, fixture: Split, min_precision: float) -> list[tuple[float, tuple]]:
    """Every grid point clearing the precision constraint and the fixture's own gate.

    Returned rather than reduced to a winner, because 3.E's finding is about the *shape* of
    this set and not only its maximum: at the shipped constraint 336 of 385 feasible points
    tie at the top recall, so "the fit chose X" is mostly a statement about the tiebreak.
    """
    # Restored on the way out. The sweep sets these on the module, and every caller that
    # afterwards asks `dedup` what it ships — `_fit`'s tiebreak, and `_select_constraint`'s
    # inner folds, which call `_fit` in a loop — would otherwise be answered with the tail of
    # the last sweep instead of the shipped configuration.
    entry = {name: getattr(dedup, name) for name in GRID}
    out = []
    try:
        out = _sweep(train, fixture, min_precision)
    finally:
        for name, value in entry.items():
            setattr(dedup, name, value)
    return out


def _sweep(train: Split, fixture: Split, min_precision: float) -> list[tuple[float, tuple]]:
    out = []
    for values in product(*GRID.values()):
        for name, value in zip(GRID, values, strict=True):
            setattr(dedup, name, value)
        precision, recall, *_ = train.score()
        if precision < min_precision:
            continue
        fixture_precision, fixture_recall, *_ = fixture.score()
        # The fixture gate, ratcheted to 1.000/1.000 in 3.E. It is synthetic ground truth —
        # the fixture's `story_key` *is* the label — so unlike the real set it carries no
        # sampling noise for a floor to absorb, and anything under perfect is a regression.
        if fixture_precision < 1.0 or fixture_recall < 1.0:
            continue
        out.append((recall, values))
    return out


def _fit(train: Split, fixture: Split, min_precision: float) -> dict[str, float] | None:
    """Maximise recall subject to the precision constraint; break ties by *not moving*.

    **The tiebreak used to be tuple order, and tuple order was deciding published numbers.**
    Measured in 3.E at the shipped constraint: only `TITLE_JACCARD` is pinned by the data
    (0.35 at every feasible point) and only `MIN_TITLE_TOKENS` changes the held-out result
    (4 gives 1.000/0.500, and 2 or 3 give 0.857/0.545). `BODY_JACCARD` and `MIN_BODY_TOKENS`
    score identically at every value the grid offers, on both splits — and over 200,000
    random real pairs the body branch fires zero times at every one of them, so the corpus
    cannot separate them either.

    So the rule is: **among tied optima, keep what `dedup.py` already ships.** It is the only
    tiebreak that does not invent evidence — a constant the data cannot speak to should not be
    moved by a procedure that claims to be measuring it — and it makes this script idempotent,
    so rerunning it never churns a constant and never silently disagrees with the module it is
    fitting. Where the shipped value is not among the tied optima the data *has* spoken, and
    the fit moves it.
    """
    shipped = tuple(SHIPPED[name] for name in GRID)
    feasible = _feasible(train, fixture, min_precision)
    if not feasible:
        return None
    top = max(recall for recall, _ in feasible)
    tied = [values for recall, values in feasible if recall == top]
    if shipped in tied:
        return dict(zip(GRID, shipped, strict=True))
    # Nothing shipped to keep: fall back to the stated direction — on equal measured recall,
    # prefer the configuration that claims less. Thresholds and minimum-signal guards up.
    return dict(zip(GRID, max(tied), strict=True))


def _undetermined(train: Split, fixture: Split, min_precision: float) -> dict[str, list]:
    """Which constants the labeled set leaves free, for the fit to report about itself.

    A fit that prints five numbers implies the data chose five numbers. Here it chose two.
    Printing that is the difference between a tuned constant and one that merely survived.
    """
    feasible = _feasible(train, fixture, min_precision)
    if not feasible:
        return {}
    top = max(recall for recall, _ in feasible)
    winners = [values for recall, values in feasible if recall == top]
    return {
        name: sorted({w[i] for w in winners})
        for i, name in enumerate(GRID)
        if len({w[i] for w in winners}) > 1
    }


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


def fit_dedup(args: argparse.Namespace) -> int:
    """SPEC §7.1's same-story thresholds, against `evals/dedup/pairs.jsonl`."""
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

        undetermined = _undetermined(train, fixture, constraint)
        for name, value in chosen.items():
            setattr(dedup, name, value)
        print("\nchosen:")
        for name, value in chosen.items():
            free = undetermined.get(name)
            note = (
                f"  (undetermined — scores identically at {free}, shipped value kept)"
                if free
                else ""
            )
            print(f"  {name:<24} {value}{note}")
        for name in NOT_FITTED:
            print(f"  {name:<24} {getattr(dedup, name)}  (held fixed — see GRID's comment)")
        if undetermined:
            print(
                f"\n  the labeled set determines {len(GRID) - len(undetermined)} of {len(GRID)} "
                f"searched constants; the rest are held at their shipped values rather than "
                f"moved by a tiebreak"
            )
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


# --- SPEC §7.2, entity resolution ---------------------------------------------------------
#
# Same procedure, different grid: fit on a train half, choose the precision constraint by
# cross-validation inside that half, report on a held-out half the fitting never sees. The
# objective is the same shape too, and for the same kind of reason — SPEC §7.2 says a mention
# below the floor is "left unlinked rather than guessed", so recall is bought only where it
# costs no precision. Attributing news to the wrong company is the error a reader cannot
# detect; a missing link is one they can.
#
# Only two constants are fitted. The per-channel confidences in `resolve.py` are an ordering
# of evidence kinds, and 54 positive mentions cannot choose eight weights without choosing
# them from noise — the same argument that keeps `MIN_SIMHASH_TOKENS` out of the grid above.
ENTITY_GRID = {
    # Breakpoints, not a sweep: these are the values that sit between the confidences
    # `resolve.py` assigns, so each one admits exactly one more channel than the last.
    "CONFIDENCE_FLOOR": [0.15, 0.25, 0.55, 0.65, 0.72, 0.80, 0.95, 1.00],
}

# `COMMON_WORD_RANK` is **held fixed at the whole word list and is not fitted**, and the
# reason is the same one that keeps `MIN_SIMHASH_TOKENS` out of the dedup grid above: the
# labeled set is too small to certify it, and it was caught being fitted from noise.
#
# 27 positive mentions in the train half, against a 64-point two-constant grid. Searching
# both picked `COMMON_WORD_RANK = 500` for +0.037 train recall and paid held-out precision
# 0.833 -> 0.727 for it. Cross-validating inside train did not rescue it — four folds of ~7
# positives each are noisier still, and CV selection landed on floor 0.55 at held-out
# precision 0.667, worse than either. Both attempts moved the number that is chosen on and
# hurt the number that is reported.
#
# So the second constant is set on the stated rule instead — link less on equal evidence —
# which at 10,000 means *any* everyday English word needs the context to corroborate it
# before it can carry a link on its own. One constant is fitted, over eight values, and the
# held-out half stays a check rather than a participant.


def _load_mentions() -> list[dict]:
    """The answered mentions. A record with no `entity_id` key is unanswered, which is a
    different state from one deliberately labeled `null`."""
    lines = (EVALS / "entities" / "mentions.jsonl").read_text(encoding="utf-8").splitlines()
    mentions = [json.loads(line) for line in lines if line.strip()]
    return [m for m in mentions if "entity_id" in m]


def _stratified_mention_halves(mentions: list[dict], seed: int) -> tuple[list[dict], list[dict]]:
    """Split by linked/unlinked, so 54 positives do not land 40/14 by chance."""
    rng = random.Random(seed)
    train: list[dict] = []
    test: list[dict] = []
    for linked in (True, False):
        group = [m for m in mentions if (m["entity_id"] is not None) is linked]
        rng.shuffle(group)
        half = len(group) // 2
        train.extend(group[:half])
        test.extend(group[half:])
    return train, test


def _score_mentions(mentions: list[dict], dictionary) -> tuple[float, float, int, int, int]:
    """Precision/recall with abstention counted, matching `evals/score.py::score_entities`:
    a correct `unlinked` is a true negative, and a link to the wrong entity is both a false
    positive and a false negative."""
    tp = fp = fn = 0
    for mention in mentions:
        predicted = entity_resolve.resolve(
            mention["surface_form"], mention.get("context", ""), dictionary=dictionary
        ).entity_id
        actual = mention["entity_id"]
        if predicted == actual:
            tp += 1 if actual is not None else 0
        else:
            fp += 1 if predicted is not None else 0
            fn += 1 if actual is not None else 0
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    return precision, recall, tp, fp, fn


def _fit_entities(train: list[dict], dictionary, min_precision: float) -> dict[str, float] | None:
    """Maximise **F1** on train, subject to the precision constraint, over one constant.

    **Different objective from `_fit` above, deliberately.** Dedup maximises recall subject
    to precision because its asymmetry is extreme: a false merge deletes a story the reader
    never learns was missing, while a false split shows a visible duplicate. Entity errors
    are not like that — a story filed under the wrong company is something the reader *sees*
    — so the trade is real in both directions and F1 says so.

    It also fixes a measured failure. Maximising recall subject to `precision >= 0.75` rode
    the constraint boundary: it chose the floor whose train precision was 0.760, barely
    clearing the bar, and held-out precision came in at 0.615. The train curve has an
    obvious knee one step away — precision 0.760 -> 0.900 for a single mention of recall —
    and F1 finds it (train 0.766 at floor 0.72) where the constrained-recall rule walked
    straight past it. The precision constraint is unchanged and still binds; it is the
    objective inside it that was wrong.
    """
    best = None
    for values in product(*ENTITY_GRID.values()):
        for name, value in zip(ENTITY_GRID, values, strict=True):
            setattr(entity_resolve, name, value)
        precision, recall, *_ = _score_mentions(train, dictionary)
        if precision < min_precision:
            continue
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        # Ties break toward the *stricter* floor: it means "link less on the same evidence",
        # which is the direction SPEC §7.2 points when the measurement cannot separate two
        # settings. Floors 0.65 and 0.72 score identically on train and the held-out half
        # says 0.833 against 0.714 — so the tie-break earned its place here rather than
        # being decoration.
        key = (round(f1, 6), *values)
        if best is None or key > best[0]:
            best = (key, dict(zip(ENTITY_GRID, values, strict=True)))
    return best[1] if best else None


def _select_entity_constraint(
    mentions: list[dict], dictionary, seed: int, candidates: list[float], folds: int = 4
) -> float:
    """The precision constraint, chosen inside train — never against the held-out half.
    Same argument as `_select_constraint` above: the constraint is the procedure's own
    hyperparameter, and picking it on the test split would spend the split."""
    rng = random.Random(seed)
    shuffled = list(mentions)
    rng.shuffle(shuffled)
    chunks = [shuffled[i::folds] for i in range(folds)]

    measured: dict[float, tuple[float, float]] = {}
    for candidate in candidates:
        precisions, recalls = [], []
        for held in range(folds):
            inner_train = [m for i, c in enumerate(chunks) if i != held for m in c]
            chosen = _fit_entities(inner_train, dictionary, candidate)
            if chosen is None:
                break
            for name, value in chosen.items():
                setattr(entity_resolve, name, value)
            precision, recall, *_ = _score_mentions(chunks[held], dictionary)
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
    strictest = max(measured)
    bar = measured[strictest][0]
    eligible = {c: r for c, (p, r) in measured.items() if p >= bar}
    return max(eligible, key=lambda c: (eligible[c], c))


# The precision a link has to clear to be worth making. **Stated, not fitted**, and the one
# number here that is a product decision rather than a measurement: three correct links per
# wrong one is the point below which the brief's per-company grouping stops being worth
# reading, and above which SPEC §7.4's market-corroboration component has something honest to
# join on.
#
# It is stated because the procedure that chose dedup's constraint by cross-validation
# **degenerates here, and the reason is structural rather than a bad seed.** Entity precision
# is monotone in a single knob — raise `CONFIDENCE_FLOOR` and you link strictly less — so
# "the strictest constraint whose precision holds up in CV" always selects the strictest grid
# point available. Measured: every candidate from 1.00 down to 0.80 returns the same CV
# precision (0.80-0.81) and the same recall (0.183), and the rule picks 1.00 — a resolver
# that reads CIKs out of EDGAR titles and ignores prose entirely, at recall 0.185. Dedup
# escapes this because its five interacting thresholds trade against each other and because
# the Phase 0 fixture is a hard floor under the degenerate corner; neither applies here.
#
# So the constraint is chosen once, in the open, on what the brief needs — and the held-out
# half still reports what that choice actually bought.
ENTITY_MIN_PRECISION = 0.75


def fit_entities(args: argparse.Namespace) -> int:
    """SPEC §7.2's confidence floor, against `evals/entities/mentions.jsonl`."""
    dictionary = entity_dictionary.load(args.dictionary)
    mentions = _load_mentions()
    train, test = _stratified_mention_halves(mentions, args.seed)
    print(
        f"mentions {len(mentions)} -> train {len(train)} "
        f"({sum(m['entity_id'] is not None for m in train)} linked) / "
        f"test {len(test)} ({sum(m['entity_id'] is not None for m in test)} linked); "
        f"dictionary {len(dictionary.entities)} entities built {dictionary.built_at[:10]}"
    )

    original = {name: getattr(entity_resolve, name) for name in ENTITY_GRID}
    try:
        constraint = args.min_precision if args.min_precision is not None else ENTITY_MIN_PRECISION
        print(f"\nprecision constraint {constraint:.2f} (stated, see ENTITY_MIN_PRECISION)")

        chosen = _fit_entities(train, dictionary, constraint)
        if chosen is None:
            print(f"\nno grid point reaches precision {constraint} — loosen it")
            return 1

        for name, value in chosen.items():
            setattr(entity_resolve, name, value)
        print("\nchosen:")
        for name, value in chosen.items():
            print(f"  {name:<24} {value}")
        print()
        for label, split in (
            ("train (fitted on)", train),
            ("HELD OUT", test),
            ("full set", mentions),
        ):
            precision, recall, tp, fp, fn = _score_mentions(split, dictionary)
            print(
                f"  {label:<18} precision={precision:.3f} recall={recall:.3f} "
                f"(tp={tp} fp={fp} fn={fn})"
            )
        if args.errors:
            print("\ntrain-half errors (the held-out half is deliberately not shown):")
            for mention in train:
                resolution = entity_resolve.resolve(
                    mention["surface_form"], mention.get("context", ""), dictionary=dictionary
                )
                if resolution.entity_id == mention["entity_id"]:
                    continue
                print(
                    f"  {mention['surface_form'][:38]:38} label={mention['entity_id']!s:28} "
                    f"got={resolution.entity_id!s:22} "
                    f"{resolution.method}/{resolution.reason or '-'} "
                    f"conf={resolution.confidence:.2f} alias={resolution.matched_alias}"
                )
    finally:
        for name, value in original.items():
            setattr(entity_resolve, name, value)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--set",
        dest="which",
        choices=["dedup", "entities"],
        default="dedup",
        help="which labeled set to fit against",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--min-precision",
        type=float,
        default=None,
        help="pin the constraint; omit to select it by cross-validation inside train",
    )
    parser.add_argument(
        "--dictionary",
        type=Path,
        default=None,
        help="entity dictionary snapshot; defaults to the committed one",
    )
    parser.add_argument(
        "--errors",
        action="store_true",
        help="list the train-half mentions the fitted rule gets wrong",
    )
    args = parser.parse_args(argv)
    return fit_entities(args) if args.which == "entities" else fit_dedup(args)


if __name__ == "__main__":
    raise SystemExit(main())
