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

# `silver.story_clusters` has no body of its own — the head's text lives in
# `silver.articles`, reached through `canonical_article_id`. This is the same join
# `brief/read.py::read_clusters` makes for the snippet under each headline, and it has to be
# the same text or the `input_hash` computed here would not match the one `enrich/run.py`
# produced, and the join to `predictions.jsonl` would silently find nothing.
#
# **This query is why 4B.G never ran.** It was written selecting a `snippet` column that
# `silver.story_clusters` does not have, and nothing caught it: the sampler has no test, it is
# the only caller, and the failure needs a real Athena catalog to surface. Found in 5.0 by
# running it — `COLUMN_NOT_FOUND: line 1:45: Column 'snippet' cannot be resolved`.
SAMPLE_SQL = """
SELECT c.cluster_id,
       c.title,
       c.publisher_domain,
       a.body_text AS snippet,
       c.article_count
FROM silver.story_clusters c
LEFT JOIN silver.articles a
  ON a.article_id = c.canonical_article_id
WHERE c.title IS NOT NULL AND c.title <> ''
"""


def _distinct_by_input(rows: list[dict]) -> list[dict]:
    """One row per distinct enrichment *question*, not per cluster.

    Clustering runs on a rolling 72-hour window, so the same story is re-clustered under a new
    `cluster_id` on each of three consecutive days (`spark/jobs/cluster.py`). Its head text does
    not change, so all three produce the same `input_hash` — which is what `predictions.jsonl`
    is keyed on and what `score.py` joins through.

    **Found by running the first draw.** 100 rows came back and scored `n=95`: five stories had
    been drawn twice (Dolly Parton, the Walmart payments story, and three others), and the
    labels dict silently collapsed each pair. Nothing was wrong with the score — the labels
    agreed — but the sample was 5% smaller than it claimed, and a disagreement between two
    labels for the same question would have been resolved by file order.
    """
    seen: set[tuple[str, str, str]] = set()
    distinct = []
    for row in rows:
        key = (
            row.get("title") or "",
            row.get("publisher_domain") or "",
            row.get("snippet") or "",
        )
        if key in seen:
            continue
        seen.add(key)
        distinct.append(row)
    return distinct


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
    rows = _distinct_by_input(result.rows)
    print(f"{len(rows)} distinct heads ({len(result.rows) - len(rows)} re-clustered duplicates)")
    buckets = _strata(rows)
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
