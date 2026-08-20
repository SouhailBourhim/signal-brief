#!/usr/bin/env python3
"""Draw candidate article pairs from real `silver.articles` for hand-labeling. SPEC §7.1.

Emits `evals/dedup/candidates.jsonl` with **no `same_story` key**. Labeling is moving a
line into `pairs.jsonl` with the answer filled in, so the scorer never has to reason about
a null and the line count left in `candidates.jsonl` is a visible progress bar.

Two things this deliberately does not do.

**It does not pre-fill an answer.** `hamming` and `jaccard` are used to *stratify* — to
find the pairs where the decision is hard — and never to guess. SPEC §12 wants labels
written before the matcher is tuned, so that the published precision/recall describes a
rule the labels judged rather than a rule that shaped them.

**It does not let one source own the sample.** `sec.gov` is 63% of the corpus (3.0's
measurement), and EDGAR pairs are pathological under the current rule: 82% of random ones
clear `SAME_STORY_JACCARD` because an EDGAR body is filing metadata, not prose. A uniform
sample would therefore be mostly one degenerate case, and a published number computed over
it would describe filings rather than the pipeline. So pairs are also stratified by what
kind of sources they join, with a cap on any one class — the filings stay represented,
because that is where the rule fails and hiding it would be the dishonest move, but they do
not crowd out the syndication cases §7.1 actually exists to catch.

Because the near/borderline strata sit on the current rule's decision boundary, a third
uniform-random stratum is drawn too. Recall measured only near the boundary is a flattering
number; the random stratum is what keeps the headline honest.

    uv run python evals/sample_pairs.py --n 200
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core.dedup import NEAR_DUPLICATE_DISTANCE, content_tokens, jaccard
from signal_core.hashing import hamming
from signal_core.timeutil import utc_now

EVALS = Path(__file__).parent

# Derived from the rule's own threshold rather than hardcoded, and deliberately wider: the
# pairs worth a human's attention are the ones just outside the rule's reach as well as the
# ones just inside it. If 3.B moves the threshold, this band follows it.
NEAR_BAND = NEAR_DUPLICATE_DISTANCE + 6

# Straddles `SAME_STORY_JACCARD` (0.25) from both sides. These are the judgement calls.
BORDERLINE_BAND = (0.10, 0.35)

# How many of each. Weighted toward borderline because that is where a threshold is decided.
STRATUM_TARGETS = {"near": 0.30, "borderline": 0.40, "random": 0.30}

# No pair class may exceed this share of a stratum. 0.4 leaves filings clearly represented
# without letting them become the measurement.
MAX_CLASS_SHARE = 0.4


def _kind(source_id: str) -> str:
    if source_id.startswith("edgar"):
        return "filing"
    if source_id == "hackernews":
        return "hn"
    return "news"


def _pair_class(a: dict[str, Any], b: dict[str, Any]) -> str:
    return "x".join(sorted((_kind(a["source_id"]), _kind(b["source_id"]))))


def _is_self_pair(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Two rows carrying one `article_id`.

    This should be impossible: `normalize_window` MERGEs on `article_id` with
    `WHEN NOT MATCHED THEN INSERT`. It is not — `silver.articles` holds 132 duplicate ids
    across 2,849 rows, found by this sampler emitting an article paired with itself
    (docs/runbooks/phase-3.md, 3.A). Skipped here rather than deduplicated upstream, so the
    defect stays visible in `read_articles` and in the brief's `articles_in` count instead
    of being quietly papered over by the labeling tool.
    """
    return a["article_id"] == b["article_id"]


def _pair_id(a: dict[str, Any], b: dict[str, Any]) -> str:
    """Order-independent, so the same pair drawn twice is recognisably the same pair."""
    lo, hi = sorted((a["article_id"], b["article_id"]))
    return f"{lo}::{hi}"


def _already_labeled() -> set[str]:
    """Pair ids already answered in `pairs.jsonl`, so a re-run never re-asks a question."""
    path = EVALS / "dedup" / "pairs.jsonl"
    if not path.exists():
        return set()
    return {
        json.loads(line)["pair_id"]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _stratify(
    articles: list[dict[str, Any]], rng: random.Random
) -> dict[tuple[str, str], list[tuple[int, int]]]:
    """Every pair, bucketed by (stratum, pair class), reservoir-sampled to bound memory.

    All pairs really are enumerated. At the measured corpus size that is ~3.6M jaccards in
    about three seconds (docs/runbooks/phase-3.md), and the alternative — random draws
    filtered by band — would need an unreasonable number of draws to fill the `near`
    stratum, which is only ~2% of pairs.
    """
    tokens = [content_tokens(f"{a['title']} {a['body_text']}") for a in articles]
    buckets: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    seen: dict[tuple[str, str], int] = defaultdict(int)
    reservoir = 2000

    for i in range(len(articles)):
        for j in range(i + 1, len(articles)):
            a, b = articles[i], articles[j]
            if _is_self_pair(a, b):
                continue
            # Byte-identical reprints are removed by `exact_dedup` before clustering ever
            # sees them, so labeling one tests nothing the rule actually decides.
            if a["content_hash"] == b["content_hash"]:
                continue

            distance = hamming(a["simhash"], b["simhash"])
            if distance <= NEAR_BAND:
                stratum = "near"
            else:
                overlap = jaccard(tokens[i], tokens[j])
                stratum = (
                    "borderline" if BORDERLINE_BAND[0] <= overlap < BORDERLINE_BAND[1] else "random"
                )

            key = (stratum, _pair_class(a, b))
            seen[key] += 1
            if len(buckets[key]) < reservoir:
                buckets[key].append((i, j))
            else:
                # Reservoir sampling: every pair in the stream has an equal chance of
                # surviving, so a bucket that saw two million pairs is not biased toward
                # whichever ones happened to be enumerated first.
                r = rng.randrange(seen[key])
                if r < reservoir:
                    buckets[key][r] = (i, j)
    return buckets


def _allocate(
    buckets: dict[tuple[str, str], list[tuple[int, int]]], n: int
) -> list[tuple[int, int]]:
    """Fill each stratum's quota, capping any single pair class, then spend the remainder
    on whatever is left rather than returning short."""
    chosen: list[tuple[int, int]] = []
    for stratum, share in STRATUM_TARGETS.items():
        quota = round(n * share)
        classes = {k[1]: v for k, v in buckets.items() if k[0] == stratum and v}
        if not classes:
            continue
        cap = max(1, int(quota * MAX_CLASS_SHARE))
        per_class = max(1, quota // len(classes))

        taken: list[tuple[int, int]] = []
        for pairs in classes.values():
            taken.extend(pairs[: min(cap, per_class)])
        # Under quota because some class ran dry: top up from the rest, still capped.
        if len(taken) < quota:
            for pairs in classes.values():
                extra = pairs[min(cap, per_class) : cap]
                taken.extend(extra[: quota - len(taken)])
                if len(taken) >= quota:
                    break
        chosen.extend(taken[:quota])
    return chosen


def _focus_pairs(
    articles: list[dict[str, Any]], n: int, rng: random.Random
) -> list[tuple[int, int]]:
    """Pairs from the sub-population where a same-story pair is *possible at all*.

    The first draw returned 194 pairs containing exactly one positive, which makes recall a
    statement about a single example rather than a measurement. The cause is structural, not
    a sampling bug: this corpus is 63% SEC filings, each a distinct company's distinct
    filing, and genuine syndication across three RSS feeds inside 72 hours is rare.

    So this mode restricts to the non-filing articles — where two sources can cover one
    event — and ranks pairs by IDF-weighted **title** token overlap. Same-event coverage
    shares names ("OpenAI", "Hugging Face") even when the prose differs completely.

    Two properties keep this from being cherry-picking. It ranks on titles only, while
    `is_same_story` decides on title *and* body, so it is not simply re-finding what the
    rule already merges. And the pairs are labeled blind, tagged `focus`, and scored as
    their own stratum — the base rate stays measurable from the `random` stratum, and a
    precision computed over an enriched sample is never reported as if it were the base
    rate.
    """
    pool = [i for i, a in enumerate(articles) if _kind(a["source_id"]) != "filing"]
    titles = {i: content_tokens(articles[i]["title"]) for i in pool}

    frequency: dict[str, int] = defaultdict(int)
    for tokens in titles.values():
        for token in tokens:
            frequency[token] += 1
    total = len(pool) or 1

    scored: list[tuple[float, tuple[int, int]]] = []
    for x in range(len(pool)):
        for y in range(x + 1, len(pool)):
            i, j = pool[x], pool[y]
            if _is_self_pair(articles[i], articles[j]):
                continue
            shared = titles[i] & titles[j]
            if not shared:
                continue
            # Rare shared words carry the signal; "the AI startup" does not.
            weight = sum(math.log(total / frequency[t]) for t in shared)
            if weight > 0:
                scored.append((weight, (i, j)))

    scored.sort(key=lambda item: (-item[0], item[1]))
    del rng  # ranked, not sampled — kept in the signature for symmetry with _stratify
    return [pair for _, pair in scored[:n]]


def _record(a: dict[str, Any], b: dict[str, Any], stratum: str, origin: str) -> dict[str, Any]:
    return {
        "pair_id": _pair_id(a, b),
        "a": {"title": a["title"], "body": a["body_text"], "publisher": a["publisher_domain"]},
        "b": {"title": b["title"], "body": b["body_text"], "publisher": b["publisher_domain"]},
        # No `same_story`. That is the human's job, and its absence is what marks this line
        # as unanswered.
        "origin": origin,
        # Not scored. Kept so 3.E can report *where* the rule fails rather than only that
        # it does — a precision figure that is fine on news and catastrophic on filings is
        # two facts, and the average of them is neither.
        "stratum": stratum,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=200, help="how many candidates to draw")
    parser.add_argument("--window-hours", type=int, default=72)
    parser.add_argument("--seed", type=int, default=0, help="fixed, so a draw is reproducible")
    parser.add_argument("--origin", default=None, help="defaults to silver-<YYYY-MM>")
    parser.add_argument("--out", type=Path, default=EVALS / "dedup" / "candidates.jsonl")
    parser.add_argument(
        "--focus",
        action="store_true",
        help="draw only from where same-story pairs can exist (see _focus_pairs)",
    )
    args = parser.parse_args(argv)

    from signal_core.brief.read import read_articles

    now = utc_now()
    articles, query = read_articles(now - timedelta(hours=args.window_hours), now)
    print(f"{len(articles)} articles, {query.bytes_scanned:,} bytes scanned")
    if len(articles) < 2:
        print("not enough articles to pair")
        return 1

    rng = random.Random(args.seed)
    if args.focus:
        chosen = _focus_pairs(articles, args.n, rng)
        stratum_of = dict.fromkeys(chosen, "focus")
        print(f"  focus       {len(chosen):>5} ranked by IDF-weighted title overlap")
    else:
        buckets = _stratify(articles, rng)
        for (stratum, pair_class), pairs in sorted(buckets.items()):
            print(f"  {stratum:<11} {pair_class:<15} {len(pairs):>5} sampled")
        chosen = _allocate(buckets, args.n)
        stratum_of = {pair: stratum for (stratum, _), pairs in buckets.items() for pair in pairs}

    origin = args.origin or f"silver-{now:%Y-%m}"
    labeled = _already_labeled()

    records, emitted = [], set()
    for i, j in chosen:
        record = _record(articles[i], articles[j], stratum_of[(i, j)], origin)
        if record["pair_id"] in labeled or record["pair_id"] in emitted:
            continue
        emitted.add(record["pair_id"])
        records.append(record)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    by_stratum: dict[str, int] = defaultdict(int)
    for record in records:
        by_stratum[record["stratum"]] += 1
    print(f"\nwrote {len(records)} candidates to {args.out}")
    print("  " + "  ".join(f"{k}={v}" for k, v in sorted(by_stratum.items())))
    print(
        f"\nlabel by moving a line into {EVALS / 'dedup' / 'pairs.jsonl'} with "
        '"same_story": true|false added. Rule: evals/dedup/README.md'
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
