#!/usr/bin/env python3
"""Draw candidate entity mentions from real `silver.articles` for hand-labeling. SPEC §7.2.

Emits `evals/entities/mentions.jsonl` with **no `entity_id` key**. Labeling is filling it
in — with a canonical id, or with `null` when the correct answer is *unlinked*.

**This sampler does not use the entity dictionary, and that is the point.** Candidates come
from a purely lexical proper-noun heuristic, so the labeled set contains mentions the
resolver will miss as well as ones it will find. Sampling from dictionary hits instead
would publish recall-given-a-candidate: a number that looks like recall, is always higher
than recall, and silently excludes every company the dictionary has never heard of. SPEC
§7.2 names private companies with no ticker as a hard case; they are exactly the ones a
dictionary-seeded sample would drop.

It also means the labels exist before `entities/resolve.py` does, which is what
`evals/entities/README.md` asks for: a rule written before labeling cannot be tuned to
flatter an implementation that has not been written.

EDGAR is capped for a different reason than in `sample_pairs.py`. An EDGAR title carries
its company name *and* its CIK — `6-K - Haleon plc (0001900304) (Filer)` — so those
mentions are structurally free to resolve and trivially correct. Letting them fill the
sample would publish an accuracy figure earned on the easy half of the corpus.

    uv run python evals/sample_mentions.py --n 300
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core.timeutil import utc_now

EVALS = Path(__file__).parent

CONTEXT_CHARS = 200

# Runs of capitalised words, optionally closed by a corporate suffix. Deliberately loose:
# over-generating is cheap (a human answers "not a company" in a second) while a missed
# surface form is a mention that can never be labeled and therefore never counted.
_PROPER_NOUN = re.compile(
    r"\b(?:[A-Z][\w&.'-]*)(?:[ ](?:of|for|and|de|van|der)?[ ]?[A-Z][\w&.'-]*){0,4}"
    r"(?:,?[ ](?:Inc|Corp|Corporation|Co|Ltd|LLC|LP|PLC|plc|NV|SA|AG|GmbH|Group|Holdings)\.?)?"
)
_TICKER = re.compile(r"\$[A-Z]{1,5}\b")

# Words that start sentences constantly and are never a company on their own. A single-word
# candidate matching one of these is dropped; a multi-word one ("The Verge") survives.
_SENTENCE_STARTERS = frozenset(
    [
        "The",
        "A",
        "An",
        "This",
        "That",
        "These",
        "Those",
        "It",
        "He",
        "She",
        "They",
        "We",
        "You",
        "I",
        "If",
        "When",
        "While",
        "After",
        "Before",
        "But",
        "And",
        "Or",
        "So",
        "Then",
        "Now",
        "Today",
        "Yesterday",
        "Tomorrow",
        "For",
        "To",
        "In",
        "On",
        "At",
        "By",
        "With",
        "From",
        "As",
        "Is",
        "Are",
        "Was",
        "Were",
        "Be",
        "Been",
        "Being",
        "Has",
        "Have",
        "Had",
        "Will",
        "Would",
        "Could",
        "Should",
        "May",
        "Might",
        "Its",
        "Their",
        "His",
        "Her",
        "Our",
        "Your",
        "My",
        "What",
        "Why",
        "How",
        "Where",
        "Who",
        "Which",
        "There",
        "Here",
        "All",
        "Some",
        "Many",
        "Most",
        "More",
        "Less",
        "Other",
        "New",
        "Old",
        "First",
        "Last",
        "Next",
        "One",
        "Two",
        "Three",
        "Show",
        "Ask",
        "Tell",
        "Get",
        "Make",
        "Use",
    ]
)

# Enough that a labeler is not staring at "AI" a hundred times, few enough that a genuinely
# frequent company still appears more than once.
MAX_PER_SURFACE_FORM = 4

# Share of the sample allowed to come from filings. See the module docstring.
MAX_FILING_SHARE = 0.25


def _kind(source_id: str) -> str:
    if source_id.startswith("edgar"):
        return "filing"
    if source_id == "hackernews":
        return "hn"
    return "news"


def _candidates(text: str) -> list[tuple[str, int, int]]:
    """(surface_form, char_start, char_end) for every proper-noun-ish span."""
    found: list[tuple[str, int, int]] = []
    for match in _TICKER.finditer(text):
        found.append((match.group(), match.start(), match.end()))
    for match in _PROPER_NOUN.finditer(text):
        surface = match.group().strip().rstrip(",")
        if not surface or len(surface) < 2:
            continue
        # Single capitalised word that is just a sentence opening: not worth a judgement.
        if " " not in surface and surface in _SENTENCE_STARTERS:
            continue
        # All-caps single tokens are usually acronyms in headlines (AI, CEO, SEC); keep the
        # ones long enough to plausibly be a name, drop the two-letter noise.
        if surface.isupper() and len(surface) <= 2:
            continue
        found.append((surface, match.start(), match.start() + len(surface)))
    return found


def _context(text: str, start: int, end: int) -> str:
    lo = max(0, start - CONTEXT_CHARS)
    hi = min(len(text), end + CONTEXT_CHARS)
    return ("…" if lo else "") + text[lo:hi].replace("\n", " ") + ("…" if hi < len(text) else "")


def _already_labeled(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        json.loads(line)["mention_id"]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=300)
    parser.add_argument("--window-hours", type=int, default=72)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--origin", default=None, help="defaults to silver-<YYYY-MM>")
    parser.add_argument("--out", type=Path, default=EVALS / "entities" / "mentions.jsonl")
    args = parser.parse_args(argv)

    from signal_core.brief.read import read_articles

    now = utc_now()
    articles, query = read_articles(now - timedelta(hours=args.window_hours), now)
    print(f"{len(articles)} articles, {query.bytes_scanned:,} bytes scanned")

    rng = random.Random(args.seed)
    rng.shuffle(articles)

    origin = args.origin or f"silver-{now:%Y-%m}"
    labeled = _already_labeled(args.out)
    seen_surface: dict[str, int] = defaultdict(int)
    by_kind: dict[str, int] = defaultdict(int)
    filing_cap = int(args.n * MAX_FILING_SHARE)

    records: list[dict[str, Any]] = []
    for article in articles:
        if len(records) >= args.n:
            break
        kind = _kind(article["source_id"])
        if kind == "filing" and by_kind["filing"] >= filing_cap:
            continue

        text = f"{article['title']}\n{article['body_text']}"
        spans = _candidates(text)
        rng.shuffle(spans)
        for surface, start, end in spans[:3]:  # at most three per article, for variety
            if len(records) >= args.n:
                break
            if seen_surface[surface] >= MAX_PER_SURFACE_FORM:
                continue
            mention_id = f"{article['article_id']}:{start}"
            if mention_id in labeled:
                continue
            seen_surface[surface] += 1
            by_kind[kind] += 1
            published = article["published_at"] or article["fetched_at"]
            records.append(
                {
                    "mention_id": mention_id,
                    "article_id": article["article_id"],
                    # On the record because the labeling rule pins a renamed company to the
                    # entity valid at *publication* date — that is what `dim_entities`
                    # SCD2 is for, and the scorer needs the date to query it as-of.
                    "published_at": published.isoformat() if published else None,
                    "surface_form": surface,
                    "char_start": start,
                    "char_end": end,
                    "context": _context(text, start, end),
                    # No `entity_id`. Absent means unanswered; `null` once answered means
                    # deliberately unlinked, which is a correct answer and is scored as one.
                    "origin": origin,
                }
            )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"\nwrote {len(records)} candidate mentions to {args.out}")
    print("  " + "  ".join(f"{k}={v}" for k, v in sorted(by_kind.items())))
    print('\nlabel by adding "entity_id": "TICKER" or null. Rule: evals/entities/README.md')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
