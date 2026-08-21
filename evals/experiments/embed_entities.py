#!/usr/bin/env python3
"""Do context embeddings link what the lexical resolver refuses? SPEC §7.2, ADR-0009.

SPEC §7.2 names the fix for the common-word class directly — "cosine similarity between
article context and entity description embeddings" — and `resolve.py` records taking a
documented decision not to reach for it yet. This measures whether it would have worked.

Same standing as `embed_dedup.py`: a measurement, not a stage. Nothing in `src/` imports it,
`make eval` does not run it, and it is meant to be run in a throwaway environment.

    /tmp/embedvenv/bin/python evals/experiments/embed_entities.py

## What "the entity description" has to be, and why that is the finding

The dictionary has no descriptions. `warehouse/entities/dictionary.json.gz` is SEC's
company-ticker file plus a Wikidata slice, and an entry is a canonical name, a ticker, a CIK,
a rank and a list of aliases — no prose. So the description is **synthesised** from those
fields, which is the honest version of what this codebase can do today, and the gap between
that and a real description is itself part of the verdict.

## The candidate set is where the lexical resolver stops, not where it starts

The point is not to re-run the resolver with a different threshold. It is to take the
mentions the resolver **locates and then declines to link** — a real alias match, vetoed by
the common-word rule or held under the confidence floor — and ask whether the context can
break the tie the word alone cannot. That is exactly the class SPEC §7.2 hands to embeddings.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

EVALS = Path(__file__).resolve().parents[1]
ROOT = EVALS.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(EVALS))


from fit_thresholds import _load_mentions, _stratified_mention_halves  # noqa: E402

from signal_core.entities import dictionary as dict_module  # noqa: E402
from signal_core.entities import resolve as entity_resolve  # noqa: E402

# The grid, dense where the decision is. Context-against-name cosines run lower than
# sentence-against-sentence ones — a headline and a company name are different kinds of
# string — so this starts lower than the dedup grid.
COSINE_GRID = [round(0.10 + 0.02 * i, 2) for i in range(40)]  # 0.10 .. 0.88

CONSTRAINTS = [1.0, 0.95, 0.90, 0.85, 0.80]

# 3.C's `ENTITY_MIN_PRECISION` reasoning applies here unchanged: entity precision is monotone
# in a single knob, so "the strictest constraint that survives CV" always picks the strictest
# grid point and lands on a resolver that links almost nothing. 3.C stated the constraint
# instead of cross-validating it, and this reuses the number 3.C stated.
STATED_CONSTRAINT = 0.85


def _describe(entity) -> str:
    """A description, from a dictionary that has none.

    SPEC §7.2 says "entity description embeddings" and there are no descriptions here, so
    this builds the best sentence the available fields support. Naming what it is missing
    matters more than the template: a real description would say what the company *does*,
    which is the signal that separates `Apple` the company from `apple` the fruit, and none
    of the fields below carry it.
    """
    parts = [entity.canonical_name]
    if entity.ticker:
        parts.append(f"traded as {entity.ticker}")
    parts.append("a public company" if entity.entity_type == "public" else "a company")
    return ", ".join(parts)


def _candidates(mention: dict, dictionary) -> list[str]:
    """Every entity the dictionary's alias index reaches from this span.

    Deliberately wider than `resolve` allows itself: complete-name matches *and* prefix
    matches, common words included. Narrowing here would hand the embedding the lexical
    rule's own answer and then congratulate it for agreeing.
    """
    surface = mention["surface_form"]
    tokens = dict_module.strip_legal_suffix(dict_module.normalize(surface))
    found: list[str] = []
    for start in range(len(tokens)):
        for end in range(len(tokens), start, -1):
            alias = dictionary.lookup(tokens[start:end])
            if alias is None:
                continue
            found.extend(alias.completes)
            found.extend(alias.starts[:5])
    seen, unique = set(), []
    for entity_id in found:
        if entity_id not in seen and entity_id in dictionary.entities:
            seen.add(entity_id)
            unique.append(entity_id)
    return unique[:20]


def _confusion(rows: list[tuple[str | None, str | None]]) -> tuple[float, float, int, int, int]:
    """`evals/score.py::score_entities`' accounting, reused exactly.

    A correct abstention is a true negative; a link to the wrong entity is both a false
    positive and a false negative. Scoring this any other way would let a resolver that
    confidently mislabels everything outrank one that abstains.
    """
    tp = fp = fn = 0
    for predicted, actual in rows:
        if predicted == actual:
            tp += 1 if actual is not None else 0
        else:
            fp += 1 if predicted is not None else 0
            fn += 1 if actual is not None else 0
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    return precision, recall, tp, fp, fn


def _predict(scored: list[tuple[str, float]], threshold: float) -> str | None:
    """Argmax over candidates, above the floor. Ties are not broken — an exact tie between
    two entities is the `ambiguous` case, and guessing it is the error SPEC §7.2 forbids."""
    if not scored:
        return None
    best = max(scored, key=lambda pair: pair[1])
    if best[1] < threshold:
        return None
    rivals = [s for entity_id, s in scored if entity_id != best[0]]
    if rivals and max(rivals) == best[1]:
        return None
    return best[0]


def main(argv: list[str] | None = None) -> int:
    from sentence_transformers import SentenceTransformer

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default="all-MiniLM-L6-v2")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args(argv)

    mentions = _load_mentions()
    dictionary = dict_module.load()
    train, test = _stratified_mention_halves(mentions, args.seed)
    print(f"model {args.model}")
    print(
        f"mentions {len(mentions)} -> train {len(train)} "
        f"({sum(m['entity_id'] is not None for m in train)} linked) / "
        f"test {len(test)} ({sum(m['entity_id'] is not None for m in test)} linked)"
    )

    candidates = {m["mention_id"]: _candidates(m, dictionary) for m in mentions}
    reachable = sum(1 for m in mentions if candidates[m["mention_id"]])
    correct_reachable = sum(
        1
        for m in mentions
        if m["entity_id"] is not None and m["entity_id"] in candidates[m["mention_id"]]
    )
    linked = sum(1 for m in mentions if m["entity_id"] is not None)
    # The ceiling, stated before any threshold is fitted. An argmax over candidates cannot
    # link an entity the alias index never proposes, so this bounds recall from above no
    # matter how good the encoder is — and it is the number that decides whether the entity
    # verdict is about embeddings at all.
    print(
        f"  {reachable} mentions reach at least one candidate; the labeled entity is among "
        f"them for {correct_reachable}/{linked} linked mentions"
    )
    print(f"  -> recall ceiling for ANY context-scoring rule: {correct_reachable / linked:.3f}")

    entity_ids = sorted({e for ids in candidates.values() for e in ids})
    model = SentenceTransformer(args.model)
    print(f"encoding {len(entity_ids)} entity descriptions and {len(mentions)} contexts...")
    entity_vectors = dict(
        zip(
            entity_ids,
            model.encode(
                [_describe(dictionary.entities[e]) for e in entity_ids],
                batch_size=64,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            ),
            strict=True,
        )
    )
    context_vectors = dict(
        zip(
            [m["mention_id"] for m in mentions],
            model.encode(
                [m.get("context", "") or m["surface_form"] for m in mentions],
                batch_size=64,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            ),
            strict=True,
        )
    )

    scored = {
        m["mention_id"]: [
            (e, float(context_vectors[m["mention_id"]] @ entity_vectors[e]))
            for e in candidates[m["mention_id"]]
        ]
        for m in mentions
    }

    def rows(
        group: list[dict], threshold: float, hybrid: bool
    ) -> list[tuple[str | None, str | None]]:
        out = []
        for m in group:
            lexical = entity_resolve.resolve(
                m["surface_form"], m.get("context", ""), dictionary=dictionary
            ).entity_id
            embedded = _predict(scored[m["mention_id"]], threshold)
            # Stage order: the lexical channels are higher-precision evidence (a CIK read off
            # a filing is not an inference at all), so the embedding only answers where they
            # abstain. The reverse order would let a cosine overrule a stated identifier.
            predicted = lexical if (hybrid and lexical is not None) else embedded
            out.append((predicted, m["entity_id"]))
        return out

    results: dict[str, dict] = {}
    for rule, hybrid in (("embed", False), ("hybrid", True)):
        best = None
        for threshold in COSINE_GRID:
            precision, recall, *_ = _confusion(rows(train, threshold, hybrid))
            if precision < STATED_CONSTRAINT:
                continue
            # 3.C's objective, reused: F1 under the constraint, not recall. A mention filed
            # under the wrong company is something the reader sees, so the trade is real both
            # ways — unlike a false merge, which deletes a story invisibly.
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            if best is None or (f1, threshold) > best[0]:
                best = ((f1, threshold), threshold)
        print(f"\n{rule} (constraint {STATED_CONSTRAINT}, objective F1):")
        if best is None:
            print(f"  no threshold reaches precision {STATED_CONSTRAINT} on train")
            results[rule] = {"fitted": None}
            continue
        threshold = best[1]
        print(f"  context-entity cosine >= {threshold}")
        results[rule] = {"fitted": {"cosine": threshold}}
        for label, group in (("train (fitted on)", train), ("HELD OUT", test)):
            precision, recall, tp, fp, fn = _confusion(rows(group, threshold, hybrid))
            print(
                f"  {label:<18} precision={precision:.3f} recall={recall:.3f} "
                f"(tp={tp} fp={fp} fn={fn})"
            )
            key = "train" if label.startswith("train") else "test"
            results[rule][key] = dict(precision=precision, recall=recall, tp=tp, fp=fp, fn=fn)

    print("\nlexical (shipped constants, the row to beat):")
    results["lexical"] = {"fitted": "shipped"}
    for label, group in (("train (fitted on)", train), ("HELD OUT", test)):
        precision, recall, tp, fp, fn = _confusion(
            [
                (
                    entity_resolve.resolve(
                        m["surface_form"], m.get("context", ""), dictionary=dictionary
                    ).entity_id,
                    m["entity_id"],
                )
                for m in group
            ]
        )
        print(
            f"  {label:<18} precision={precision:.3f} recall={recall:.3f} (tp={tp} fp={fp} fn={fn})"
        )
        key = "train" if label.startswith("train") else "test"
        results["lexical"][key] = dict(precision=precision, recall=recall, tp=tp, fp=fp, fn=fn)

    # The whole reason SPEC §7.2 names embeddings: the class the word alone cannot settle.
    print("\nthe common-word class, mention by mention:")
    common = [
        m
        for m in mentions
        if any(
            dictionary.word_rank(token) is not None
            for token in dict_module.normalize(m["surface_form"])
        )
    ]
    threshold = results.get("embed", {}).get("fitted", {})
    threshold = threshold.get("cosine") if isinstance(threshold, dict) else None
    for m in common:
        lexical = entity_resolve.resolve(
            m["surface_form"], m.get("context", ""), dictionary=dictionary
        ).entity_id
        embedded = _predict(scored[m["mention_id"]], threshold) if threshold else None
        verdict = "same" if lexical == embedded else f"{lexical} -> {embedded}"
        mark = "ok " if embedded == m["entity_id"] else "BAD"
        print(
            f"  [{mark}] {m['surface_form'][:26]:<26} label={m['entity_id']!s:<12} "
            f"lexical={lexical!s:<12} embed={embedded!s:<12} {verdict}"
        )

    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "model": args.model,
                    "seed": args.seed,
                    "recall_ceiling": correct_reachable / linked,
                    "results": results,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
