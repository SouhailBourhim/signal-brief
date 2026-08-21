#!/usr/bin/env python3
"""Does an embedding same-story rule beat the lexical one? SPEC §7.1 stage 3, ADR-0009.

**This is a measurement, not a shipped stage.** Nothing in `src/` imports it and nothing in
`make eval` runs it. It exists so ADR-0009's verdict rests on numbers that can be
reproduced, and so a later phase that wants to revisit the decision starts from a harness
rather than from an argument.

Run it in a throwaway environment — the whole question is whether the dependency is worth
adding, and adding it to `pyproject.toml` in order to ask would answer it by assumption:

    uv venv /tmp/embedvenv --python 3.12
    VIRTUAL_ENV=/tmp/embedvenv uv pip install 'sentence-transformers>=3,<6' --torch-backend=cpu
    /tmp/embedvenv/bin/python evals/experiments/embed_dedup.py

## The comparison is held fair by construction

Every choice that could flatter the challenger is taken from `evals/fit_thresholds.py`
rather than made here:

- **The same seeded, label-stratified halves.** `_stratified_halves(pairs, seed=0)` is
  imported, not reimplemented, so both rules are fitted on the same 126 pairs and reported
  on the same held-out 126.
- **The same objective**: maximise recall subject to a precision constraint, because a false
  merge deletes a story invisibly and a false split shows a cheap duplicate.
- **The same constraint-selection procedure**: 4-fold cross-validation *inside* the train
  half, over the same candidate list, under the same stated rule — never trade precision,
  but take recall that costs none. Handing the challenger the incumbent's fitted 0.90
  instead would be judging it under a bar chosen for a different rule; running CV for it is
  the fair version, and it is the same code path, imported.
- **The same hard fixture gate**: precision 1.000 and recall >= 0.9 on the 55 synthetic Phase
  0 pairs. 3.B added it after a real-corpus fit quietly lost the rewrite case, and a
  challenger that buys recall by giving that up has not won anything.

## What is measured

Three rules, all over the same embeddings:

1. `embed` — cosine on titles, cosine on bodies, thresholds fitted independently, either can
   fire. The shape of `dedup.decide`, with cosine where Jaccard is.
2. `hybrid` — `dedup.decide(...) or embed(...)`. The realistic deployment: SPEC §7.1 calls
   embeddings *stage 3*, a widening pass after the lexical stages, not a replacement.
3. `lexical` — `dedup.decide` at the shipped constants, for the row to beat.

`--model` defaults to `all-MiniLM-L6-v2`, the sentence-transformers default and the
cheapest thing that could plausibly win; a 384-dim, 90 MB model. If it loses by a wide
margin the question of a larger one is still open, and the printed margin is what says
whether it is worth asking.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

EVALS = Path(__file__).resolve().parents[1]
ROOT = EVALS.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(EVALS))

import numpy as np  # noqa: E402
from fit_thresholds import (  # noqa: E402
    _load,
    _stratified_halves,
)

from signal_core import dedup  # noqa: E402

# The candidate constraints 3.B cross-validated over, reused unchanged so the challenger is
# selected by the same procedure rather than a friendlier one.
CONSTRAINTS = [1.0, 0.95, 0.90, 0.85, 0.80]

# Cosine thresholds. Sentence-transformer cosines over short news text bunch high — unrelated
# headlines routinely sit at 0.2-0.4 — so the grid is dense where the decision actually is.
COSINE_GRID = [round(0.30 + 0.02 * i, 2) for i in range(35)]  # 0.30 .. 0.98

# The same minimum-signal principle the lexical rule uses: below these there is not enough
# text for a similarity to mean anything, and the branch abstains rather than guessing. Taken
# from `dedup` so the two rules abstain on exactly the same pairs.
MIN_TITLE_TOKENS = dedup.MIN_TITLE_TOKENS
MIN_BODY_TOKENS = dedup.MIN_BODY_TOKENS


def _clean(text: str) -> str:
    """The same boilerplate stripping the lexical rule gets.

    Not a courtesy — 3.B measured that an EDGAR body is the feed's own field names plus an
    accession number, so two unrelated filings share most of their vocabulary by
    construction. Feeding the raw text to the encoder instead would be handing the challenger
    a corpus the incumbent was fixed for, and the resulting comparison would measure the
    cleaning, not the embedding.
    """
    return dedup.strip_boilerplate(text)


def _texts(pairs: list[dict]) -> list[str]:
    out: list[str] = []
    for pair in pairs:
        for side in ("a", "b"):
            out.append(_clean(pair[side]["title"]))
            out.append(_clean(pair[side]["body"]))
    return out


def _encode(texts: list[str], model_name: str) -> np.ndarray:
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    return model.encode(
        texts,
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )


def _cosines(pairs: list[dict], vectors: np.ndarray) -> list[tuple[float, float, bool, bool, bool]]:
    """Per pair: title cosine, body cosine, the two readability guards, and the identifier veto.

    Vectors are L2-normalised, so a dot product is the cosine.
    """
    out = []
    for index, pair in enumerate(pairs):
        base = index * 4
        a_title, a_body, b_title, b_body = vectors[base : base + 4]
        prepared_a = dedup.prepare(pair["a"]["title"], pair["a"]["body"])
        prepared_b = dedup.prepare(pair["b"]["title"], pair["b"]["body"])
        title_readable = (
            len(prepared_a.title) >= MIN_TITLE_TOKENS and len(prepared_b.title) >= MIN_TITLE_TOKENS
        )
        body_readable = (
            len(prepared_a.body) >= MIN_BODY_TOKENS and len(prepared_b.body) >= MIN_BODY_TOKENS
        )
        vetoed = bool(
            prepared_a.identifiers
            and prepared_b.identifiers
            and prepared_a.identifiers != prepared_b.identifiers
        )
        out.append(
            (
                float(a_title @ b_title),
                float(a_body @ b_body),
                title_readable,
                body_readable,
                vetoed,
            )
        )
    return out


def _predict_embed(
    row: tuple[float, float, bool, bool, bool], title_t: float, body_t: float
) -> bool:
    """The shape of `dedup.decide`, with cosine where Jaccard is.

    The identifier veto and both minimum-signal guards are inherited rather than reimplemented
    or dropped. They are structural — statements about when there is anything to compare at
    all — where the thresholds are the thing under test. `embed_corpus.py` measures what
    withholding the veto costs: over random real pairs it is the difference between a
    corpus-level false-merge rate of 0.025% and one of 0.347%, entirely in EDGAR filings.
    """
    title_cos, body_cos, title_ok, body_ok, vetoed = row
    if vetoed:
        return False
    if title_ok and title_cos >= title_t:
        return True
    return body_ok and body_cos >= body_t


def _confusion(predicted: list[bool], actual: list[bool]) -> tuple[float, float, int, int, int]:
    tp = sum(1 for p, a in zip(predicted, actual, strict=True) if p and a)
    fp = sum(1 for p, a in zip(predicted, actual, strict=True) if p and not a)
    fn = sum(1 for p, a in zip(predicted, actual, strict=True) if not p and a)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    return precision, recall, tp, fp, fn


def _lexical(pairs: list[dict]) -> list[bool]:
    return [
        dedup.is_same_story(p["a"]["title"], p["a"]["body"], p["b"]["title"], p["b"]["body"])
        for p in pairs
    ]


def _fit_cosine(
    rows: list[tuple[float, float, bool, bool, bool]],
    actual: list[bool],
    fixture_rows: list[tuple[float, float, bool, bool, bool]],
    fixture_actual: list[bool],
    lexical: list[bool] | None,
    fixture_lexical: list[bool] | None,
    min_precision: float,
) -> tuple[float, float] | None:
    """Maximise recall subject to `MIN_PRECISION`, with the fixture gate as a hard filter.

    `lexical` non-None makes this the hybrid fit: the embedding branch is fitted knowing the
    lexical rule already fires on some pairs, which is what stage 3 actually faces.
    """
    best = None
    for title_t in COSINE_GRID:
        for body_t in COSINE_GRID:
            predicted = [_predict_embed(row, title_t, body_t) for row in rows]
            if lexical is not None:
                predicted = [e or lx for e, lx in zip(predicted, lexical, strict=True)]
            precision, recall, *_ = _confusion(predicted, actual)
            if precision < min_precision:
                continue
            fixture_predicted = [_predict_embed(row, title_t, body_t) for row in fixture_rows]
            if fixture_lexical is not None:
                fixture_predicted = [
                    e or lx for e, lx in zip(fixture_predicted, fixture_lexical, strict=True)
                ]
            f_precision, f_recall, *_ = _confusion(fixture_predicted, fixture_actual)
            if f_precision < 1.0 or f_recall < 0.9:
                continue
            # Ties broken toward the stricter thresholds: on equal measured recall, prefer
            # the rule that claims less. Same direction as the lexical fitter's tiebreak.
            key = (recall, title_t, body_t)
            if best is None or key > best[0]:
                best = (key, (title_t, body_t))
    return best[1] if best else None


def _select_constraint(
    rows: list[tuple[float, float, bool, bool, bool]],
    actual: list[bool],
    lexical: list[bool] | None,
    fixture_rows: list[tuple[float, float, bool, bool, bool]],
    fixture_actual: list[bool],
    fixture_lexical: list[bool] | None,
    seed: int,
    folds: int = 4,
) -> float:
    """`fit_thresholds._select_constraint`, applied to the cosine grid.

    Reimplemented rather than imported because the incumbent's version fits by mutating
    `dedup`'s module constants and there is nothing here to mutate — but the shape, the
    candidate list, the fold count and the selection rule are all the same, so the challenger
    is chosen the way the incumbent was.
    """
    rng = random.Random(seed)
    order = list(range(len(rows)))
    rng.shuffle(order)
    chunks = [order[i::folds] for i in range(folds)]

    measured: dict[float, tuple[float, float]] = {}
    for candidate in CONSTRAINTS:
        precisions, recalls = [], []
        for held in range(folds):
            inner = [i for f, chunk in enumerate(chunks) if f != held for i in chunk]
            chosen = _fit_cosine(
                [rows[i] for i in inner],
                [actual[i] for i in inner],
                fixture_rows,
                fixture_actual,
                [lexical[i] for i in inner] if lexical is not None else None,
                fixture_lexical,
                candidate,
            )
            if chosen is None:
                break
            title_t, body_t = chosen
            predicted = [_predict_embed(rows[i], title_t, body_t) for i in chunks[held]]
            if lexical is not None:
                predicted = [p or lexical[i] for p, i in zip(predicted, chunks[held], strict=True)]
            precision, recall, *_ = _confusion(predicted, [actual[i] for i in chunks[held]])
            precisions.append(precision)
            recalls.append(recall)
        if len(recalls) != folds:
            continue
        measured[candidate] = (sum(precisions) / folds, sum(recalls) / folds)
        print(
            f"  constraint {candidate:.2f}: cv precision={measured[candidate][0]:.3f} "
            f"recall={measured[candidate][1]:.3f}"
        )

    if not measured:
        return 1.0
    strictest = max(measured)
    bar = measured[strictest][0]
    eligible = {c: r for c, (p, r) in measured.items() if p >= bar}
    return max(eligible, key=lambda c: (eligible[c], c))


def _frontier(
    rows: dict[str, list],
    labels: dict[str, list[bool]],
    lexical: dict[str, list[bool]],
) -> None:
    """The precision/recall frontier at every constraint, train and held out side by side.

    Printed as a diagnostic, never as a selection: choosing the constraint by reading the
    held-out column is exactly the thing the split exists to prevent. It is here because
    ADR-0009 has to say *why* the verdict is what it is, and "the challenger's precision does
    not survive the split at any constraint" is a different claim from "it lost at 0.90".
    """
    print("\nfrontier (diagnostic — the constraint is NOT chosen from this table):")
    print(f"  {'constraint':<11} {'thresholds':<16} {'train p/r':<16} HELD OUT p/r")
    for candidate in CONSTRAINTS:
        chosen = _fit_cosine(
            rows["train"],
            labels["train"],
            rows["fixture"],
            labels["fixture"],
            None,
            None,
            candidate,
        )
        if chosen is None:
            print(f"  {candidate:<11.2f} unreachable under the fixture gate")
            continue
        title_t, body_t = chosen
        cells = []
        for split in ("train", "test"):
            predicted = [_predict_embed(row, title_t, body_t) for row in rows[split]]
            precision, recall, *_ = _confusion(predicted, labels[split])
            cells.append(f"{precision:.3f} / {recall:.3f}")
        print(f"  {candidate:<11.2f} {f'{title_t} / {body_t}':<16} {cells[0]:<16} {cells[1]}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default="all-MiniLM-L6-v2")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--json", type=Path, help="write the result table here")
    args = parser.parse_args(argv)

    real = _load(fixture=False)
    fixture = _load(fixture=True)
    train_pairs, test_pairs = _stratified_halves(real, args.seed)

    print(f"model {args.model}")
    print(
        f"real pairs {len(real)} -> train {len(train_pairs)} "
        f"({sum(p['same_story'] for p in train_pairs)} positive) / "
        f"test {len(test_pairs)} ({sum(p['same_story'] for p in test_pairs)} positive); "
        f"fixture {len(fixture)}"
    )

    print("encoding...", flush=True)
    all_pairs = train_pairs + test_pairs + fixture
    vectors = _encode(_texts(all_pairs), args.model)
    print(f"  {vectors.shape[0]} texts -> {vectors.shape[1]} dims")

    offset = 0
    rows: dict[str, list] = {}
    labels: dict[str, list[bool]] = {}
    for name, group in (
        ("train", train_pairs),
        ("test", test_pairs),
        ("fixture", fixture),
    ):
        rows[name] = _cosines(group, vectors[offset * 4 : (offset + len(group)) * 4])
        labels[name] = [p["same_story"] for p in group]
        offset += len(group)

    lexical = {
        name: _lexical(group)
        for name, group in (("train", train_pairs), ("test", test_pairs), ("fixture", fixture))
    }

    results: dict[str, dict] = {}

    predictions: dict[str, dict[str, list[bool]]] = {}
    for rule in ("embed", "hybrid"):
        use_lexical = rule == "hybrid"
        print(f"\n{rule} — selecting the precision constraint by {4}-fold CV inside train:")
        constraint = _select_constraint(
            rows["train"],
            labels["train"],
            lexical["train"] if use_lexical else None,
            rows["fixture"],
            labels["fixture"],
            lexical["fixture"] if use_lexical else None,
            args.seed,
        )
        print(f"  -> {constraint:.2f}")
        chosen = _fit_cosine(
            rows["train"],
            labels["train"],
            rows["fixture"],
            labels["fixture"],
            lexical["train"] if use_lexical else None,
            lexical["fixture"] if use_lexical else None,
            constraint,
        )
        if chosen is None:
            print(f"  no threshold pair reaches precision {constraint} under the fixture gate")
            results[rule] = {"fitted": None}
            continue
        title_t, body_t = chosen
        print(f"  title cosine >= {title_t}   body cosine >= {body_t}")
        results[rule] = {
            "constraint": constraint,
            "fitted": {"title_cosine": title_t, "body_cosine": body_t},
        }
        predictions[rule] = {}
        for split in ("train", "test", "fixture"):
            predicted = [_predict_embed(row, title_t, body_t) for row in rows[split]]
            if use_lexical:
                predicted = [e or lx for e, lx in zip(predicted, lexical[split], strict=True)]
            predictions[rule][split] = predicted
            precision, recall, tp, fp, fn = _confusion(predicted, labels[split])
            tag = {"train": "train (fitted on)", "test": "HELD OUT", "fixture": "fixture"}[split]
            print(
                f"  {tag:<18} precision={precision:.3f} recall={recall:.3f} "
                f"(tp={tp} fp={fp} fn={fn})"
            )
            results[rule][split] = dict(precision=precision, recall=recall, tp=tp, fp=fp, fn=fn)

    print("\nlexical (shipped constants, the row to beat):")
    results["lexical"] = {"fitted": "shipped"}
    for split in ("train", "test", "fixture"):
        precision, recall, tp, fp, fn = _confusion(lexical[split], labels[split])
        tag = {"train": "train (fitted on)", "test": "HELD OUT", "fixture": "fixture"}[split]
        print(
            f"  {tag:<18} precision={precision:.3f} recall={recall:.3f} (tp={tp} fp={fp} fn={fn})"
        )
        results["lexical"][split] = dict(precision=precision, recall=recall, tp=tp, fp=fp, fn=fn)

    # The recall gap is the whole question, so name the pairs rather than only counting them:
    # which held-out positives does the lexical rule miss, and does the embedding find them?
    print("\nheld-out positives the lexical rule misses:")
    embed_fit = results.get("embed", {}).get("fitted")
    for index, (pair, row, actual) in enumerate(
        zip(test_pairs, rows["test"], labels["test"], strict=True)
    ):
        if not actual or lexical["test"][index]:
            continue
        found = (
            _predict_embed(row, embed_fit["title_cosine"], embed_fit["body_cosine"])
            if embed_fit
            else False
        )
        print(
            f"  [{'FOUND' if found else '     '}] title cos {row[0]:.2f} body cos {row[1]:.2f}  "
            f"{pair['a']['title'][:52]!r} vs {pair['b']['title'][:52]!r}"
        )

    # 3.B's finding was that a pairwise precision number understates cluster damage, because
    # union-find takes a transitive closure. So the false merges get named, not counted: what
    # a rule merges wrongly is the thing that decides whether it can be let near `group_stories`.
    if "embed" in predictions:
        print("\nheld-out pairs the embedding rule merges and the labels do not:")
        for pair, predicted, actual in zip(
            test_pairs, predictions["embed"]["test"], labels["test"], strict=True
        ):
            if predicted and not actual:
                print(f"  {pair['a']['title'][:58]!r}")
                print(f"    vs {pair['b']['title'][:58]!r}   [{pair.get('stratum', '?')}]")

        # `focus` is enriched for positives and the base-rate strata are not, so one combined
        # precision describes neither. The strata that matter for clustering are the
        # representative ones: that is the pair distribution a real window presents.
        print("\nheld-out by stratum (`focus` is enriched for positives — not a base rate):")
        strata = sorted({p.get("stratum", "unsampled") for p in test_pairs})
        for stratum in strata:
            index = [
                i for i, p in enumerate(test_pairs) if p.get("stratum", "unsampled") == stratum
            ]
            actual = [labels["test"][i] for i in index]
            for name, predicted_all in (
                ("lexical", lexical["test"]),
                ("embed", predictions["embed"]["test"]),
            ):
                precision, recall, tp, fp, fn = _confusion(
                    [predicted_all[i] for i in index], actual
                )
                print(
                    f"  {stratum:<11} {name:<8} n={len(index):<4} precision={precision:.3f} "
                    f"recall={recall:.3f} (tp={tp} fp={fp} fn={fn})"
                )

    _frontier(rows, labels, lexical)

    if args.json:
        args.json.write_text(
            json.dumps({"model": args.model, "seed": args.seed, "results": results}, indent=2)
            + "\n",
            encoding="utf-8",
        )
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
