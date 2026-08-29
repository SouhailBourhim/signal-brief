#!/usr/bin/env python3
"""Would an embedding branch move dedup's recall ceiling? ADR-0009 §1; 5.C.

ADR-0009 adopted embeddings for SPEC §7.1 stage 3 and measured the case with
`sentence-transformers` — the vehicle it then rejected on packaging grounds. ADR-0016 built the
vehicle it chose instead. **This re-asks the question through the shipping vehicle**, because a
decision taken on one encoder's numbers should not be executed on another's without checking.

The published dedup figures are precision 0.962 / recall 0.568 on 252 real pairs, with a
held-out recall of 0.500 that `evals/thresholds.toml` calls the ceiling. `dedup.decide` has
three ways to say "same story"; this measures a fourth — cosine over the head text — as a
disjunction with the existing rule, which is how a branch would actually be added.

Reported as a sweep rather than a single number, because the interesting question is not "does
some threshold help" (some threshold always helps a recall figure) but "is there a threshold
that buys recall without giving back the precision the brief depends on".

    uv run python evals/experiments/embed_dedup_ollama.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from signal_core.config import Settings
from signal_core.dedup import is_same_story
from signal_core.enrich.embed import EmbeddingCache

EVALS = Path(__file__).resolve().parents[1]
FIXTURE_ORIGIN = "phase0-fixture"


def head_text(side: dict) -> str:
    """Title plus body, which is what a same-story judgement is actually made on."""
    return " ".join(f"{side.get('title') or ''} {side.get('body') or ''}".split())


def report(name: str, tp: int, fp: int, fn: int) -> None:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    print(
        f"  {name:22} precision={precision:.3f} recall={recall:.3f} f1={f1:.3f} "
        f"(tp={tp} fp={fp} fn={fn})"
    )


def main() -> int:
    settings = Settings()
    pairs = [
        json.loads(line)
        for line in (EVALS / "dedup" / "pairs.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    pairs = [p for p in pairs if p.get("origin") != FIXTURE_ORIGIN]
    print(f"{len(pairs)} real labeled pairs")

    cache = EmbeddingCache(settings.embedding_cache_path, settings.ollama_embed_model_digest)
    print(f"cache: {cache.load()} vectors")
    texts = [head_text(p[side]) for p in pairs for side in ("a", "b")]
    vectors = cache.embed(texts, settings, progress=print)
    cache.save()

    lexical, actual, sims = [], [], []
    for index, pair in enumerate(pairs):
        a, b = vectors[2 * index], vectors[2 * index + 1]
        sims.append(sum(x * y for x, y in zip(a, b, strict=True)))
        lexical.append(
            is_same_story(
                pair["a"]["title"], pair["a"]["body"], pair["b"]["title"], pair["b"]["body"]
            )
        )
        actual.append(bool(pair["same_story"]))

    positives = sum(actual)
    print(f"{positives} positive, {len(pairs) - positives} negative\n")

    print("what ships today:")
    report(
        "lexical only",
        sum(1 for lx, ac in zip(lexical, actual, strict=True) if lx and ac),
        sum(1 for lx, ac in zip(lexical, actual, strict=True) if lx and not ac),
        sum(1 for lx, ac in zip(lexical, actual, strict=True) if not lx and ac),
    )

    print("\nlexical OR cosine >= t:")
    for threshold in (0.99, 0.98, 0.97, 0.96, 0.95, 0.93, 0.90, 0.85, 0.80):
        predicted = [lx or s >= threshold for lx, s in zip(lexical, sims, strict=True)]
        report(
            f">= {threshold:.2f}",
            sum(1 for pr, ac in zip(predicted, actual, strict=True) if pr and ac),
            sum(1 for pr, ac in zip(predicted, actual, strict=True) if pr and not ac),
            sum(1 for pr, ac in zip(predicted, actual, strict=True) if not pr and ac),
        )

    missed = [s for s, lx, ac in zip(sims, lexical, actual, strict=True) if ac and not lx]
    if missed:
        missed.sort()
        print(f"\ncosine on the {len(missed)} positives the lexical rule misses:")
        print(f"  min {missed[0]:.4f}  median {missed[len(missed) // 2]:.4f}  max {missed[-1]:.4f}")
    negatives = sorted((s for s, ac in zip(sims, actual, strict=True) if not ac), reverse=True)
    print(f"\nhighest cosines among true negatives: {', '.join(f'{s:.4f}' for s in negatives[:8])}")
    corpus_false_merge_rate(cache, settings)
    return 0


def corpus_false_merge_rate(cache: EmbeddingCache, settings: Settings, sample: int = 2000) -> None:
    """The measurement 252 labeled pairs structurally cannot make. `corpus_merge_rate.py` §1.

    3.B's most expensive lesson: pairwise precision 1.000 sat beside a single cluster holding
    59% of the corpus. `group_stories` takes a transitive closure, so one false edge merges two
    components permanently, and a per-pair error rate far too small for 252 pairs to detect is
    still thousands of edges per window.

    A random pair from one window is same-story with probability near zero — dedup_ratio is
    1.1 — so anything a rule merges here is a false merge to within a rounding error. Only
    `sample` heads are embedded; the pairs are drawn among them, so 2,000 vectors answer a
    question about 2 million pairs.
    """
    import random

    from signal_core.brief.read import read_window_heads

    heads, _ = read_window_heads()
    rng = random.Random(0)
    drawn = rng.sample(heads, min(sample, len(heads)))
    print(f"\ncorpus false-merge rate over {len(drawn)} real heads:")
    vectors = cache.embed([h["title"] for h in drawn], settings)
    cache.save()

    thresholds = (0.99, 0.95, 0.90, 0.85, 0.80)
    hits = dict.fromkeys(thresholds, 0)
    pairs = 0
    for i in range(len(drawn)):
        for j in range(i + 1, len(drawn)):
            pairs += 1
            similarity = sum(x * y for x, y in zip(vectors[i], vectors[j], strict=True))
            for threshold in thresholds:
                if similarity >= threshold:
                    hits[threshold] += 1
    for threshold in thresholds:
        merged = hits[threshold]
        print(f"  >= {threshold:.2f}   {merged:>9,} / {pairs:,} pairs = {merged / pairs:.6f}")


if __name__ == "__main__":
    raise SystemExit(main())
