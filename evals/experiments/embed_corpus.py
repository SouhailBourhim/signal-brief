#!/usr/bin/env python3
"""The false-merge rate over random real pairs, lexical against embedding. ADR-0009.

**Why this exists and the pairwise eval does not answer it.** 3.B's most expensive lesson
was that a 252-pair labeled set cannot bound an error rate that `group_stories` applies to
millions of pairs, and that `union-find` takes a transitive closure — so one false edge
merges two components permanently, and a rate too small for the eval to see is still
thousands of edges per window. The pairwise numbers said precision 1.000 while a single
cluster held 59% of the corpus. 3.D watched the same thing happen again at a simhash
threshold the labeled set could not distinguish at all.

So a challenger that wins on 252 labeled pairs has not yet been measured on the thing that
actually broke. This measures it the way 3.B did: draw random pairs from a real window,
where the base rate of same-story is near zero, and count what each rule merges.

Two steps, because the two halves need different environments — the dump needs the repo's
AWS stack, the scoring needs an encoder that is deliberately not in the repo:

    uv run python evals/experiments/embed_corpus.py dump --out /tmp/articles.json
    /tmp/embedvenv/bin/python evals/experiments/embed_corpus.py score --articles /tmp/articles.json
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import timedelta
from pathlib import Path

EVALS = Path(__file__).resolve().parents[1]
ROOT = EVALS.parent
sys.path.insert(0, str(ROOT / "src"))

from signal_core import dedup  # noqa: E402

# Fitted by `embed_dedup.py` on the train half, under CV-selected constraint 1.00. Held here
# as a literal rather than re-fitted, because re-fitting against this corpus would be fitting
# on the data the measurement is supposed to judge.
TITLE_COSINE = 0.68
BODY_COSINE = 0.98


def dump(args: argparse.Namespace) -> int:
    """Pull a real window out of `silver.articles` through Athena, exactly as the brief does."""
    from signal_core.brief.read import read_articles
    from signal_core.timeutil import utc_now

    now = utc_now()
    articles, query = read_articles(now - timedelta(hours=args.window_hours), now)
    print(f"{len(articles)} articles, {query.bytes_scanned:,} bytes scanned")
    # `body_text`, not `body`. The column is named `body_text` in `silver.articles` and a
    # `.get("body")` here returned "" for all 4,298 rows — which read as "the simhash and body
    # branches never fire on real data" until the eligibility counts were printed and turned
    # out to be zero rather than small. Emitted under the name `dedup` uses so the two cannot
    # drift apart again.
    slim = [
        {
            "article_id": a["article_id"],
            "title": a.get("title") or "",
            "body": a.get("body_text") or "",
        }
        for a in articles
    ]
    empty = sum(1 for a in slim if not a["body"])
    print(f"  {empty:,} of {len(slim):,} have no body text")
    args.out.write_text(json.dumps(slim), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


def score(args: argparse.Namespace) -> int:
    from sentence_transformers import SentenceTransformer

    articles = json.loads(args.articles.read_text(encoding="utf-8"))
    print(f"{len(articles)} articles")

    rng = random.Random(args.seed)
    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    while len(pairs) < args.n:
        i, j = rng.randrange(len(articles)), rng.randrange(len(articles))
        if i == j:
            continue
        key = (min(i, j), max(i, j))
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    print(f"{len(pairs):,} random pairs, seed {args.seed}")

    # A random pair from a 4,300-article window is same-story with probability near zero —
    # 3.B measured `dedup_ratio` at 1.01, so under ~50 of the 9.2M possible pairs are genuine.
    # Anything either rule merges here is therefore a false merge to within a rounding error,
    # which is what makes an unlabeled sample usable for a precision-shaped question.
    prepared = [dedup.prepare(a["title"], a["body"]) for a in articles]

    model = SentenceTransformer(args.model)
    titles = model.encode(
        [dedup.strip_boilerplate(a["title"]) for a in articles],
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    bodies = model.encode(
        [dedup.strip_boilerplate(a["body"]) for a in articles],
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    print(f"encoded {2 * len(articles):,} texts")

    lexical_merges: list[tuple[int, int]] = []
    embed_merges: list[tuple[int, int]] = []
    for i, j in pairs:
        if dedup.decide(prepared[i], prepared[j]):
            lexical_merges.append((i, j))
        # The identifier veto is a structural guard, not a lexical threshold: two documents
        # that each carry identifiers and carry *different* ones are different documents.
        # The challenger gets it too, or the measurement compares a rule with a guard against
        # a rule without one and reports the guard as the encoder's failure.
        vetoed = (
            args.veto
            and prepared[i].identifiers
            and prepared[j].identifiers
            and prepared[i].identifiers != prepared[j].identifiers
        )
        if vetoed:
            continue
        title_ok = (
            len(prepared[i].title) >= dedup.MIN_TITLE_TOKENS
            and len(prepared[j].title) >= dedup.MIN_TITLE_TOKENS
        )
        body_ok = (
            len(prepared[i].body) >= dedup.MIN_BODY_TOKENS
            and len(prepared[j].body) >= dedup.MIN_BODY_TOKENS
        )
        if (title_ok and float(titles[i] @ titles[j]) >= args.title_cosine) or (
            body_ok and float(bodies[i] @ bodies[j]) >= args.body_cosine
        ):
            embed_merges.append((i, j))

    print()
    for name, merges in (("lexical", lexical_merges), ("embed", embed_merges)):
        rate = len(merges) / len(pairs)
        print(f"  {name:<8} {len(merges):>5} merges / {len(pairs):,} pairs = {rate:.4%}")

    # The rate is not the point on its own — the point is what it becomes once a window's
    # worth of pairs runs through it and union-find chains the result.
    n = len(articles)
    total_pairs = n * (n - 1) // 2
    print(f"\n  a {n:,}-article window holds {total_pairs:,} pairs; at these rates that is")
    for name, merges in (("lexical", lexical_merges), ("embed", embed_merges)):
        projected = total_pairs * len(merges) / len(pairs)
        print(f"    {name:<8} ~{projected:,.0f} false edges")

    only_lexical = set(lexical_merges) - set(embed_merges)
    only_embed = set(embed_merges) - set(lexical_merges)
    print(
        f"\n  {len(set(lexical_merges) & set(embed_merges))} merges in common, "
        f"{len(only_lexical)} lexical-only, {len(only_embed)} embed-only — equal counts are "
        f"not the same 50 pairs"
    )

    print("\n  what the embedding rule merges that the lexical one does not:")
    extra = sorted(only_embed)
    for i, j in extra[: args.show]:
        print(f"    title cos {float(titles[i] @ titles[j]):.2f}  {articles[i]['title'][:56]!r}")
        print(f"                     vs {articles[j]['title'][:56]!r}")
    if len(extra) > args.show:
        print(f"    ... and {len(extra) - args.show} more")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    dumper = sub.add_parser("dump", help="pull a real window out of silver.articles")
    dumper.add_argument("--window-hours", type=int, default=72)
    dumper.add_argument("--out", type=Path, required=True)
    dumper.set_defaults(func=dump)

    scorer = sub.add_parser("score", help="count false merges over random pairs")
    scorer.add_argument("--articles", type=Path, required=True)
    scorer.add_argument("--n", type=int, default=200_000, help="random pairs to draw")
    scorer.add_argument("--seed", type=int, default=0)
    scorer.add_argument("--model", default="all-MiniLM-L6-v2")
    scorer.add_argument("--title-cosine", type=float, default=TITLE_COSINE)
    scorer.add_argument("--body-cosine", type=float, default=BODY_COSINE)
    scorer.add_argument("--show", type=int, default=15)
    scorer.add_argument(
        "--no-veto",
        dest="veto",
        action="store_false",
        help="withhold `decide`'s identifier veto from the embedding rule",
    )
    scorer.set_defaults(func=score)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
