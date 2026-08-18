"""Deduplication and story grouping. SPEC §7.1.

All four stages exist, cheapest first, but stage 3 is a placeholder:

  1. exact          — content hash (`exact_dedup`)
  2. near-duplicate — simhash, for reprints and light edits
  3. same-story     — LEXICAL OVERLAP IN PHASE 0; Phase 3 replaces this with
                      sentence-transformer embeddings over a 72-hour window
  4. canonical      — most authoritative, earliest-seen article becomes cluster head

Stages 2 and 3 are separate because they detect different things, measurably so. On
article-length news text a one-word edit is ~3 bits of simhash distance while a genuine
rewrite of the same event is ~21 — indistinguishable from unrelated text at ~28. Simhash
cannot see that "Northwind acquires Lumen" and "Lumen to be bought by Northwind" are one
story; content-word Jaccard scores those at 0.45 against 0.00 for unrelated text, which
is enough to group them until embeddings arrive.

`group_stories` is the seam: Phase 3 swaps the similarity function and nothing upstream
or downstream moves.
"""

from __future__ import annotations

import re
from typing import Any

from signal_core.hashing import hamming

# Simhash distance below which two texts are the same text. Measured on article-length
# news copy: light edits (one word, two words, a trailing cut, added boilerplate) land at
# 8-9 bits, while semantic rewrites and unrelated articles land at 23-29. 14 sits in the
# middle of that gap. Re-measured against `evals/dedup` in Phase 3 on real articles.
NEAR_DUPLICATE_DISTANCE = 14

# Content-word overlap above which two articles describe the same event. Measured over
# every labeled pair in `evals/dedup`: same-story pairs bottom out at 0.32, while the
# highest-scoring different-story pair reaches only 0.12. 0.25 clears the true floor with
# roughly 2x margin from the false-merge ceiling. Re-measured on real articles in Phase 3,
# where embeddings replace this outright.
SAME_STORY_JACCARD = 0.25

# Function words carry no topical signal and inflate overlap between unrelated texts.
_STOPWORDS = frozenset(
    [
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "if",
        "then",
        "than",
        "that",
        "this",
        "these",
        "those",
        "of",
        "in",
        "on",
        "at",
        "by",
        "for",
        "with",
        "from",
        "to",
        "as",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "it",
        "its",
        "it's",
        "has",
        "have",
        "had",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "said",
        "says",
        "say",
        "about",
        "after",
        "before",
        "over",
        "under",
        "into",
        "out",
        "up",
        "down",
        "no",
        "not",
    ]
)
_TOKEN = re.compile(r"[a-z0-9.'%$]+")


def content_tokens(text: str) -> frozenset[str]:
    return frozenset(t for t in _TOKEN.findall(text.lower()) if len(t) > 1 and t not in _STOPWORDS)


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def is_same_story(a_text: str, b_text: str, a_simhash: int, b_simhash: int) -> bool:
    """The single same-story decision, shared by clustering and the eval harness.

    Exists as one function so `evals/dedup` scores the decision the pipeline actually
    makes. If the eval reimplemented the rule, the published precision/recall would
    describe a system that does not exist — which is the standard way accuracy numbers in
    portfolio projects turn out to be fiction.
    """
    if hamming(a_simhash, b_simhash) <= NEAR_DUPLICATE_DISTANCE:
        return True
    return jaccard(content_tokens(a_text), content_tokens(b_text)) >= SAME_STORY_JACCARD


# Ranked by how much independent reporting they tend to originate. Phase 3 replaces this
# with something measured; it exists now so canonical selection is not arbitrary.
_AUTHORITY = {
    "reuters.com": 0.95,
    "sec.gov": 0.95,
    "arstechnica.com": 0.8,
    "techcrunch.com": 0.75,
    "theverge.com": 0.75,
}
DEFAULT_AUTHORITY = 0.5


def authority(domain: str) -> float:
    return _AUTHORITY.get(domain, DEFAULT_AUTHORITY)


def exact_dedup(articles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Collapse byte-identical reprints. Returns (kept, removed_count)."""
    seen: set[str] = set()
    kept = []
    for article in articles:
        if article["content_hash"] in seen:
            continue
        seen.add(article["content_hash"])
        kept.append(article)
    return kept, len(articles) - len(kept)


def group_stories(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group articles into story clusters. SPEC §7.1 stages 2-4.

    Union-find over two signals: simhash proximity (same text) and content-word Jaccard
    (same event). Either one merges, because they catch disjoint cases.

    O(n^2) and honestly so: at a few hundred articles per window that is microseconds,
    and the banded-LSH blocking that makes it scale belongs with the Phase 3 rewrite,
    where there will be volume to justify it.
    """
    parent = list(range(len(articles)))
    tokens = [content_tokens(f"{a['title']} {a['body_text']}") for a in articles]

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(articles)):
        for j in range(i + 1, len(articles)):
            near_identical = (
                hamming(articles[i]["simhash"], articles[j]["simhash"]) <= NEAR_DUPLICATE_DISTANCE
            )
            if near_identical or jaccard(tokens[i], tokens[j]) >= SAME_STORY_JACCARD:
                union(i, j)

    grouped: dict[int, list[dict[str, Any]]] = {}
    for index, article in enumerate(articles):
        grouped.setdefault(find(index), []).append(article)

    clusters = []
    for members in grouped.values():
        # Canonical = most authoritative, earliest-seen. The rest become
        # distinct_publisher_count, which feeds ranking instead of being discarded.
        head = min(
            members,
            key=lambda a: (-authority(a["publisher_domain"]), a["fetched_at"], a["article_id"]),
        )
        publishers = {m["publisher_domain"] for m in members}
        clusters.append(
            {
                "cluster_id": head["article_id"],
                "canonical_article_id": head["article_id"],
                "title": head["title"],
                "body_text": head["body_text"],
                "url_canonical": head["url_canonical"],
                "publisher_domain": head["publisher_domain"],
                "published_at": head["published_at"],
                "fetched_at": min(m["fetched_at"] for m in members),
                "article_count": len(members),
                "distinct_publisher_count": len(publishers),
                "publishers": sorted(publishers),
                "timestamp_flagged": head["timestamp_flagged"],
                "story_key": head.get("story_key"),
            }
        )
    return sorted(clusters, key=lambda c: (-c["distinct_publisher_count"], c["cluster_id"]))


def dedup_ratio(articles_in: int, clusters_out: int) -> float:
    """SPEC §15's headline processing metric."""
    return articles_in / clusters_out if clusters_out else 0.0
