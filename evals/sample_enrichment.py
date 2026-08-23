#!/usr/bin/env python3
"""Draw real cluster heads for hand-labeling. SPEC §7.3; 4B.G.

Emits `evals/enrichment/examples.jsonl` with **no `topic`, `summary_ok` or `extraction`
keys**. Labeling is filling them in, per `evals/enrichment/README.md`'s rule.

## The sample is stratified, because the corpus is 57% one thing

Measured before this script was written (docs/runbooks/phase-4b.md 4B.A): 5,818 of 10,186
clusters are `sec.gov`, almost all routine fund administration. A uniform draw of 100 would
therefore spend ~57 examples on `ABS-EE` and `N-PX` filings that are trivially correct to
label `sec-filing`, and would publish an accuracy figure earned on the easy half of the
corpus — the same failure `sample_mentions.py` caps EDGAR to avoid, arriving from the other
direction.

So the draw is stratified with a stated cap: enough filings that the `sec-filing` topic is
genuinely measured, and the rest weighted toward what a brief actually shows. The strata are
recorded per row so the score can be broken down later the way `dedup_by_stratum` does.

## The input hash is the join key

Each row carries the `input_hash` its head text produces under the *current* prompt version,
which is what `predictions.jsonl` is keyed on. A prompt-version bump therefore invalidates
the join, which is correct: the labels still describe the story, but the predictions no
longer describe the same question.

    uv run python evals/sample_enrichment.py --n 100
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core.config import Settings
from signal_core.enrich.prompt import PROMPT_VERSION
from signal_core.enrich.run import cluster_input
from signal_core.hashing import enrichment_cache_key
from signal_core.ops.athena import run_query

EVALS = Path(__file__).parent
OUT = EVALS / "enrichment" / "examples.jsonl"

# How many of the 100 may be SEC filings. They are 57% of the corpus and ~2% of what a brief
# shows, so neither the base rate nor zero is the right number: a fifth measures the topic
# without letting it dominate the score.
SEC_CAP_RATIO = 0.20

SAMPLE_SQL = """
SELECT cluster_id, title, publisher_domain, snippet, article_count
FROM silver.story_clusters
WHERE title IS NOT NULL AND title <> ''
"""


def _strata(rows: list[dict]) -> dict[str, list[dict]]:
    """Three strata: routine filings, corroborated stories, single-source stories.

    `article_count > 1` is the closest thing this corpus has to "a story other outlets also
    covered", and those are what the brief leads with — so they are sampled deliberately
    rather than left to a draw that would produce almost none of them.
    """
    buckets: dict[str, list[dict]] = {"sec": [], "corroborated": [], "single": []}
    for row in rows:
        if (row.get("publisher_domain") or "") == "sec.gov":
            buckets["sec"].append(row)
        elif int(row.get("article_count") or 1) > 1:
            buckets["corroborated"].append(row)
        else:
            buckets["single"].append(row)
    return buckets


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=100, help="how many examples, default 100")
    parser.add_argument("--seed", type=int, default=4, help="draw seed, recorded in the output")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    settings = Settings()

    result = run_query(SAMPLE_SQL, database=settings.athena_database)
    print(f"{len(result.rows)} clusters, {result.bytes_scanned:,} bytes, ${result.cost_usd:.6f}")
    buckets = _strata(result.rows)
    for name, rows in buckets.items():
        print(f"  {name:14} {len(rows)}")

    sec_n = min(int(args.n * SEC_CAP_RATIO), len(buckets["sec"]))
    # The rest split evenly between corroborated and single-source, falling back to whichever
    # has rows when one is thin — a fresh lake may have almost no multi-article clusters.
    rest = args.n - sec_n
    corroborated_n = min(rest // 2, len(buckets["corroborated"]))
    single_n = min(rest - corroborated_n, len(buckets["single"]))

    drawn: list[tuple[str, dict]] = []
    for stratum, count in (
        ("sec", sec_n),
        ("corroborated", corroborated_n),
        ("single", single_n),
    ):
        drawn += [(stratum, row) for row in rng.sample(buckets[stratum], count)]
    rng.shuffle(drawn)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for stratum, row in drawn:
            cluster = {
                "cluster_id": row["cluster_id"],
                "title": row.get("title") or "",
                "publisher_domain": row.get("publisher_domain") or "",
                "snippet": row.get("snippet") or "",
            }
            handle.write(
                json.dumps(
                    {
                        "input_hash": enrichment_cache_key(
                            cluster_input(cluster),
                            settings.ollama_model_digest,
                            PROMPT_VERSION,
                        ),
                        "cluster_id": cluster["cluster_id"],
                        "stratum": stratum,
                        "title": cluster["title"],
                        "publisher_domain": cluster["publisher_domain"],
                        "body": cluster["snippet"],
                        "prompt_version": PROMPT_VERSION,
                        "seed": args.seed,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    print(
        f"\nwrote {len(drawn)} to {args.out} "
        f"(sec={sec_n} corroborated={corroborated_n} single={single_n})"
    )
    print("Label each row with `topic`, `summary_ok`, and `extraction` — see the README.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
