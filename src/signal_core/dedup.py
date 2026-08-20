"""Deduplication and story grouping. SPEC §7.1.

Four stages, cheapest first:

  1. exact          — content hash (`exact_dedup`)
  2. near-duplicate — simhash over cleaned text, for reprints and light edits
  3. same-story     — token overlap, title-first (`decide`)
  4. canonical      — most authoritative, earliest-seen article becomes cluster head

Phase 3.B rebuilt stages 2 and 3 against 252 real labeled pairs, after Phase 0's rule
scored **precision 0.000 on base-rate samples** — 34 merges, every one wrong — while missing
23 of 43 genuine same-story pairs (docs/runbooks/phase-3.md, 3.0 and 3.A). Two distinct
causes, and the fixes are independent:

**Precision: the text was never cleaned.** SPEC §7.1 stage 1 says "content hash after
boilerplate stripping" and the stripping was never implemented. An EDGAR body is the feed's
own field names plus an accession number — `Filed: … AccNo: … Size: 10 KB` — so two
unrelated filings lodged on one day share most of their vocabulary by construction. 82% of
random EDGAR pairs cleared the old threshold. No threshold repairs that, and neither would
an embedding: the text describes no event.

**Recall: Jaccard punished length asymmetry.** The old rule scored one bag of words over
`title + body`. Where one source carries a headline and no body and another carries 120
tokens of prose, the union is dominated by one side and the score collapses — measured
median body-length ratio on the misses was **19.5x**, with pairs scoring title overlap 0.833
and combined 0.041. Titles are the signal that survives: on the labeled set the median title
overlap is 0.333 for pairs the old rule missed against 0.091 for pairs it wrongly merged.
So titles are compared to titles, bodies to bodies, and never pooled.

Both stages abstain rather than guess when there is too little text to judge — the same
principle as the entity resolver's confidence floor. A Jaccard over three tokens is noise,
and a rule that merges on noise deletes stories from the brief.

`decide` is the seam: it is the single same-story decision, shared by clustering and by the
eval harness, and `evals/score.py` scores it rather than a reimplementation.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any

from signal_core.hashing import hamming, simhash64

# Every constant below was fitted by `evals/fit_thresholds.py` against the labeled pairs —
# grid search on a train split, the precision constraint chosen by cross-validation inside
# that split, and the numbers reported on a held-out half that the fitting never saw. Rerun
# it after changing any of them; picking one by eye is how a threshold becomes lucky rather
# than tuned.

# Simhash distance below which two texts are the same text, over CLEANED text — markup
# stripped, feed field names and identifiers dropped. Phase 0 used 14 against raw text and
# that is where 9 of its false merges came from: two EDGAR filings are mostly identical
# boilerplate, so their raw simhashes sat well inside 14 bits of each other.
#
# **The labeled set does not distinguish this constant at all.** Every distance in the grid,
# 0 through 12, scores identically on precision and recall — because once boilerplate is
# stripped, the title path already catches everything stage 2 would have. That is a finding
# about this corpus, not a reason to delete the stage: it means the value is chosen on
# documented intent rather than measured, and 12 is the value at which SPEC §7.1's stage 2
# does its stated job of catching reprints and light edits (measured at 8-9 bits) without
# reaching semantic rewrites (~21 bits, which is stage 3's work). It costs nothing measurable
# and covers a case this corpus is too thin to contain: identical prose under a new headline,
# where the title path would miss and `exact_dedup`'s raw-text hash would too.
NEAR_DUPLICATE_DISTANCE = 12

# Title overlap above which two articles report the same event. Titles carry the signal that
# survives length asymmetry: measured across the 252 labeled pairs, the median title overlap
# is 0.333 for pairs the Phase 0 rule missed and 0.091 for pairs it wrongly merged, so the
# classes separate here even where the pooled score cannot see them at all.
TITLE_JACCARD = 0.35

# Body overlap, applied only when both bodies are substantial.
BODY_JACCARD = 0.30

# Below these, a Jaccard is noise rather than evidence — two three-token sets sharing one
# incidental word score 0.33 — so the corresponding comparison abstains instead of guessing.
# This is what stops EDGAR, whose body reduces to almost nothing once its own field names are
# removed, from being merged on the residue.
#
# `MIN_TITLE_TOKENS` and `TITLE_JACCARD` were fitted together and trade against each other:
# requiring a longer title is what makes the looser 0.35 threshold safe, because short titles
# are where a loose threshold does its damage.
MIN_TITLE_TOKENS = 4
MIN_BODY_TOKENS = 10

# The same guard for stage 2, which the first cut of 3.B forgot to give it — and that
# omission is what left a 1,575-article cluster standing after the pairwise numbers said
# precision 1.000. A simhash's discriminative power comes from having enough features to
# hash; over the ~9 tokens an EDGAR filing reduces to, collisions inside 12 bits ran at
# **1.9% of random pairs**, which over a window's 3.6M pairs is tens of thousands of false
# edges. Measured, not assumed: `evals/fit_thresholds.py` fits this alongside the rest.
MIN_SIMHASH_TOKENS = 25

# Markup, then entities, then bare URLs. The Verge's bodies are largely `<figure>`/`<img>`
# with caption and copyright attributes; EDGAR's are `<b>`-wrapped field labels. Removing
# whole tags — attributes included — is what turns both back into prose, and it is stage 1's
# "boilerplate stripping" that SPEC §7.1 specified and Phase 0 never implemented.
_TAG = re.compile(r"<[^>]*>")
_URL = re.compile(r"https?://\S+|www\.\S+")

# Identifiers, not content: CIKs and accession-number segments are long digit runs, and every
# filing has different ones, so they add noise in both directions. Short numbers survive —
# `$2.4B`, `600%`, `24 days` are things an article is actually about.
_LONG_DIGITS = re.compile(r"^\d{5,}$")

# Field names the feeds emit about themselves. These are not words anyone chose while writing
# about an event, and in this corpus they are the single largest source of shared vocabulary
# between unrelated documents. Kept separate from `_STOPWORDS` because that list is about
# English carrying no topical signal, while this one is about a specific defect in specific
# sources — and a reader deleting an entry from the wrong list would be making a different
# decision than they thought.
_FEED_BOILERPLATE = frozenset(
    [
        "filed",
        "accno",
        "filer",
        "issuer",
        "reporting",
        "subject",
        "size",
        "kb",
        "mb",
        "gb",
        "show",
        "hn",
        "ask",
        "video",
        "pdf",
        "image",
        "caption",
        "figcaption",
        "quality",
        "crop",
        "jpg",
        "png",
        "webp",
    ]
)

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


def strip_boilerplate(text: str) -> str:
    """Markup, entities and URLs out. SPEC §7.1 stage 1."""
    if not text:
        return ""
    return _URL.sub(" ", html.unescape(_TAG.sub(" ", text)))


def content_tokens(text: str) -> frozenset[str]:
    """Topical words only: markup stripped, stopwords and feed field names and long
    identifiers dropped."""
    return frozenset(
        t
        for t in _TOKEN.findall(strip_boilerplate(text).lower())
        if len(t) > 1
        and t not in _STOPWORDS
        and t not in _FEED_BOILERPLATE
        and not _LONG_DIGITS.match(t)
    )


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


@dataclass(frozen=True)
class Prepared:
    """One article reduced to exactly what the same-story decision reads.

    Separate from the article dict so the work is done once per article rather than once per
    pair: clustering compares O(n^2) pairs, and tokenizing inside that loop is what made
    Phase 0's version slow enough to notice.

    `simhash` is computed here over cleaned text, not read from `silver.articles`. The
    stored column stays useful as a **blocking** key — it is cheap and approximate, and
    blocking is allowed to be approximate because it only proposes candidates — but the
    decision must not depend on a value computed before boilerplate stripping existed.
    """

    title: frozenset[str]
    body: frozenset[str]
    simhash: int


def prepare(title: str, body: str) -> Prepared:
    clean_title = strip_boilerplate(title or "")
    clean_body = strip_boilerplate(body or "")
    return Prepared(
        title=content_tokens(title or ""),
        body=content_tokens(body or ""),
        simhash=simhash64(f"{clean_title} {clean_body}".strip()),
    )


def decide(a: Prepared, b: Prepared) -> bool:
    """The single same-story decision, shared by clustering and the eval harness.

    Exists as one function so `evals/dedup` scores the decision the pipeline actually makes.
    If the eval reimplemented the rule, the published precision/recall would describe a
    system that does not exist — which is the standard way accuracy numbers in portfolio
    projects turn out to be fiction.

    Three ways to be the same story, and each abstains rather than guesses when the evidence
    is too thin to read:

    1. Near-identical cleaned text — a reprint or a light edit.
    2. Titles that agree, when both are long enough for agreement to mean something.
    3. Bodies that agree, when both are substantial. Never pooled with titles: a headline
       against 120 tokens of prose scores near zero however well the headline matches.
    """
    a_signal, b_signal = len(a.title) + len(a.body), len(b.title) + len(b.body)
    near_readable = a_signal >= MIN_SIMHASH_TOKENS and b_signal >= MIN_SIMHASH_TOKENS
    if near_readable and hamming(a.simhash, b.simhash) <= NEAR_DUPLICATE_DISTANCE:
        return True
    title_readable = len(a.title) >= MIN_TITLE_TOKENS and len(b.title) >= MIN_TITLE_TOKENS
    if title_readable and jaccard(a.title, b.title) >= TITLE_JACCARD:
        return True
    body_readable = len(a.body) >= MIN_BODY_TOKENS and len(b.body) >= MIN_BODY_TOKENS
    return body_readable and jaccard(a.body, b.body) >= BODY_JACCARD


def is_same_story(a_title: str, a_body: str, b_title: str, b_body: str) -> bool:
    """`decide`, for callers holding raw strings — the eval harness and tests.

    Title and body are separate parameters rather than one concatenated blob because keeping
    them apart is the whole recall fix; a signature that pooled them could not express it.
    """
    return decide(prepare(a_title, a_body), prepare(b_title, b_body))


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


# A story is not 5% of everything published in three days. Above this share of the window, a
# component is evidence of a chained false merge rather than of a big news day, and it is
# dissolved back into singletons.
#
# The guard is structural, not a tuning knob, and it would be needed even at perfect measured
# precision. Union-find takes a transitive closure, so ONE bad edge merges two components
# permanently; a per-pair false-merge rate far too small for a 252-pair eval to detect is
# still tens of thousands of edges over a window's 3.6M pairs, and a few well-placed ones
# chain the lot. 3.B watched exactly that happen: pairwise precision measured 1.000 on the
# base-rate strata while a single cluster held 59% of the corpus.
#
# Dissolving rather than failing the run is the direction `evals/dedup/README.md`'s asymmetry
# points: a false split shows the reader a visible, cheap duplicate, while a false merge
# deletes a story they never learn was missing. It is also deterministic and
# order-independent, so a replay reproduces it exactly.
MAX_CLUSTER_SHARE = 0.05

# ...but never below this, so the guard cannot fire on a small window where 5% is two
# articles. It caught the Phase 0 fixture's legitimate four-publisher event on the first
# attempt, which is how the floor came to be measured rather than assumed: the largest
# genuine cluster observed in a real 72-hour window is **27** articles across 12 publishers
# (a single Disney/FCC story), and the false one it sat beside was 1,575. There is a lot of
# room between those two numbers and this sits in it.
MIN_CLUSTER_CAP = 50


@dataclass(frozen=True)
class ClusterResult:
    """Clusters, plus what the size guard had to undo.

    `dissolved` is returned rather than logged because SPEC §11 is built on the principle
    that silence is the failure mode: a run that quietly dissolved a 1,500-article cluster
    looks identical, in the brief, to a run that never formed one.
    """

    clusters: list[dict[str, Any]]
    dissolved: int = 0
    dissolved_articles: int = 0


def group_stories(articles: list[dict[str, Any]]) -> ClusterResult:
    """Group articles into story clusters. SPEC §7.1 stages 2-4.

    Union-find over two signals: simhash proximity (same text) and content-word Jaccard
    (same event). Either one merges, because they catch disjoint cases.

    O(n^2) and honestly so: at a few hundred articles per window that is microseconds,
    and the banded-LSH blocking that makes it scale belongs with the Phase 3 rewrite,
    where there will be volume to justify it.
    """
    parent = list(range(len(articles)))
    # Once per article, not once per pair — see `Prepared`.
    prepared = [prepare(a["title"], a["body_text"]) for a in articles]

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
            if decide(prepared[i], prepared[j]):
                union(i, j)

    grouped: dict[int, list[dict[str, Any]]] = {}
    for index, article in enumerate(articles):
        grouped.setdefault(find(index), []).append(article)

    # The size guard, before canonical selection: an oversized component has no meaningful
    # head to choose.
    cap = max(MIN_CLUSTER_CAP, int(len(articles) * MAX_CLUSTER_SHARE))
    dissolved = dissolved_articles = 0
    components: list[list[dict[str, Any]]] = []
    for members in grouped.values():
        if len(members) > cap:
            dissolved += 1
            dissolved_articles += len(members)
            components.extend([member] for member in members)
        else:
            components.append(members)

    clusters = []
    for members in components:
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
    return ClusterResult(
        clusters=sorted(clusters, key=lambda c: (-c["distinct_publisher_count"], c["cluster_id"])),
        dissolved=dissolved,
        dissolved_articles=dissolved_articles,
    )


def dedup_ratio(articles_in: int, clusters_out: int) -> float:
    """SPEC §15's headline processing metric."""
    return articles_in / clusters_out if clusters_out else 0.0
