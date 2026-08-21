"""The fitter must agree with the module it fits. SPEC §11, §12.

`evals/fit_thresholds.py` chooses the constants in `dedup.py`, and for most of Phase 3
nothing checked that its answer and the shipped code were the same answer. They were not:
by 3.E the fit was printing `NEAR_DUPLICATE_DISTANCE = 12` while `dedup.py` shipped 0 — the
value 3.D had removed after it chained a 45-article false cluster out of two unrelated Show
HN posts. Nothing failed, because a fitter's output is prose until someone reads it.

These are the assertions that would have caught it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "evals"))

import fit_thresholds as fit  # noqa: E402

from signal_core import dedup  # noqa: E402


@pytest.fixture
def splits():
    real = fit._load(fixture=False)
    train_pairs, _ = fit._stratified_halves(real, 0)
    return fit._split(train_pairs, 0), fit._split(fit._load(fixture=True), 0)


def test_shipped_is_captured_before_any_sweep_moves_it():
    """`SHIPPED` is read at import, and `dedup.py` is what it must equal.

    The tiebreak keeps the shipped value when the data cannot choose, so a `SHIPPED` that
    drifted from the module would silently pin the fit to a configuration nobody ships.
    """
    assert {name: getattr(dedup, name) for name in fit.GRID} == fit.SHIPPED


def test_sweeping_restores_the_module(splits):
    """`_feasible` sets every constant on `dedup` as it searches; leaving the last grid point
    in place made `_fit`'s tiebreak read the tail of its own sweep."""
    train, fixture = splits
    before = {name: getattr(dedup, name) for name in fit.GRID}
    fit._feasible(train, fixture, 0.85)
    assert {name: getattr(dedup, name) for name in fit.GRID} == before


def test_the_fit_reproduces_what_dedup_ships(splits):
    """The property that makes the published numbers mean anything: rerun the fitter and it
    returns the configuration already in the module, rather than churning constants the
    labeled set cannot distinguish."""
    train, fixture = splits
    chosen = fit._fit(train, fixture, 0.85)
    assert chosen == {name: getattr(dedup, name) for name in fit.GRID}


def test_the_constants_a_corpus_decides_are_not_in_the_grid():
    """`NEAR_DUPLICATE_DISTANCE` and `MIN_SIMHASH_TOKENS` are set from corpus-level
    measurements the pairwise objective is blind to (3.B, 3.D). In the grid, the labeled set
    scores every value identically and the tiebreak alone decides — which is how the fit came
    to recommend the distance that produced 3.D's 45-article cluster."""
    for name in fit.NOT_FITTED:
        assert name not in fit.GRID
        assert hasattr(dedup, name)


def test_the_fit_reports_what_the_labels_cannot_determine(splits):
    """A fit that prints four numbers implies the data chose four. It chose one.

    If a future labeled set genuinely determines more of them this test tightens rather than
    breaks — the assertion is that the *reporting* exists, and that `TITLE_JACCARD`, the one
    constant pinned at every feasible grid point, is not among the undetermined.
    """
    train, fixture = splits
    undetermined = fit._undetermined(train, fixture, 0.85)
    assert "TITLE_JACCARD" not in undetermined
    assert set(undetermined) <= set(fit.GRID)
    for name, values in undetermined.items():
        assert getattr(dedup, name) in values, f"{name} ships a value outside its tied optima"
