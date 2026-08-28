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
import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

from signal_core.hashing import hamming, simhash64
from signal_core.records import Article, StoryCluster
from signal_core.timeutil import ensure_utc

# Every constant below was fitted by `evals/fit_thresholds.py` against the labeled pairs —
# grid search on a train split, the precision constraint chosen by cross-validation inside
# that split, and the numbers reported on a held-out half that the fitting never saw. Rerun
# it after changing any of them; picking one by eye is how a threshold becomes lucky rather
# than tuned.

# Simhash distance below which two texts are the same text, over CLEANED text — markup
# stripped, feed field names and identifiers dropped. **Zero: exact equality of the cleaned
# simhash, and nothing looser.** The trail matters more than the value.
#
# Phase 0 used 14 against raw text, and 9 of its false merges came from it — two EDGAR
# filings are mostly identical boilerplate, so their raw simhashes sat well inside 14 bits.
# 3.B stripped the boilerplate and set 12 on documented intent, recording honestly that
# **the labeled set cannot distinguish this constant at all**: every value from 0 to 12
# scores identically on both the real pairs and the Phase 0 fixture, because once
# boilerplate is gone the title path already catches everything this stage would.
#
# 3.B also wrote down why that was a risk rather than a curiosity — a per-pair error rate
# far too small for a 252-pair eval to detect is still thousands of edges over a window's
# millions of pairs, and transitive closure chains them. **3.D watched it happen twice.**
# Reading the real brief, the lead cluster held 45 articles: Disney/FCC, a Grok exploit,
# four Show HN posts, a Pixel deal and a corgi tracker. Two of the edges holding it together:
#
#     Show HN: Markdown Buddy         vs  Meet the startup helping Wall Street...
#       title 0.00  body 0.02  hamming 12
#     Show HN: Keystroke Biometrics   vs  Show HN: Check if any of the $656M...
#       title 0.00  body 0.05  hamming 10   (224 and 111 body tokens — not a short-text case)
#
# Lowering 12 -> 10 removed the first and not the second, which is the point at which the
# right answer stops being "tune it down another bit". A 64-bit simhash over the ~150 cleaned
# tokens of a news body does not have the separation this stage assumes: measured over real
# articles clearing `MIN_SIMHASH_TOKENS`, unrelated pairs collide at 0.065% by distance 11
# and 0.9% by 14, and the tail reaches 10.
#
# At 0 the stage cannot collide by accident, and it still does the job SPEC §7.1 gives it —
# identical prose republished under a different headline, which `exact_dedup`'s raw-text hash
# misses because the headline changed the bytes. What it gives up is light edits at 8-9 bits,
# and those are precisely what the title path already catches.
#
# **This is not a claim that banded LSH is useless.** It is a claim about this corpus: six
# feeds with almost no true syndication (`dedup_ratio` 1.01). A corpus with real newswire
# reprints would justify revisiting it — with a measurement, on that corpus.
NEAR_DUPLICATE_DISTANCE = 0

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

# An EDGAR accession number, whole: `0001872100-26-000003`. Matched against text rather than
# against tokens, because `_TOKEN` excludes the hyphen and would leave three fragments that
# are indistinguishable from the CIKs in the same line.
_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")

# Field names the feeds emit about themselves. These are not words anyone chose while writing
# about an event, and in this corpus they are the single largest source of shared vocabulary
# between unrelated documents. Kept separate from `_STOPWORDS` because that list is about
# English carrying no topical signal, while this one is about a specific defect in specific
# sources — and a reader deleting an entry from the wrong list would be making a different
# decision than they thought.
#
# Public because `entities/resolve.py` needs the same list for a different reason: `Filer`,
# `Filed` and `AccNo` are also the shapes a proper-noun heuristic mistakes for company names,
# and a second copy of this list would eventually disagree with this one about what the feeds
# say about themselves.
FEED_BOILERPLATE = frozenset(
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
        and t not in FEED_BOILERPLATE
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
    # Long digit runs — accession numbers, CIKs, docket numbers. Dropped from the token sets
    # because they add nothing to a topical overlap, but kept here because they are the
    # document's *identity*, and identity is what tells two filings apart when everything
    # else about them agrees.
    identifiers: frozenset[str] = frozenset()
    # The accession numbers, whole. `identifiers` cannot hold these: `_TOKEN` has no hyphen
    # in its character class, so `0001872100-26-000003` reaches it as three unrelated digit
    # runs, indistinguishable from the CIKs sitting beside them. EDGAR's submission id
    # survives here intact because it is the one identifier that says *same filing* rather
    # than merely *some number this document contains*. See 4A.G.
    accessions: frozenset[str] = frozenset()


def identifiers(text: str) -> frozenset[str]:
    """The long digit runs `content_tokens` drops. SPEC §7.1 stage 4's tie-breaker."""
    return frozenset(
        t for t in _TOKEN.findall(strip_boilerplate(text or "").lower()) if _LONG_DIGITS.match(t)
    )


def accessions(text: str) -> frozenset[str]:
    """EDGAR accession numbers, matched whole against text tokenization would shatter.

    An accession number identifies a *submission*, and EDGAR indexes one submission under
    every CIK it concerns — so a Form 4 appears twice in the feed, once under the reporting
    person and once under the issuer, with different titles and different CIKs. Everything
    else about the pair disagrees; this is the only field that says they are one filing.
    """
    return frozenset(_ACCESSION.findall(strip_boilerplate(text or "")))


def prepare(title: str | None, body: str | None) -> Prepared:
    """`title` and `body` are nullable in `ARTICLES_DDL`, and every line below already
    coerced them — the signature said `str` while the body said `title or ""`. Widened to
    match what it does, so callers stop having to decide which of the two to believe."""
    clean_title = strip_boilerplate(title or "")
    clean_body = strip_boilerplate(body or "")
    raw = f"{title or ''} {body or ''}"
    return Prepared(
        title=content_tokens(title or ""),
        body=content_tokens(body or ""),
        simhash=simhash64(f"{clean_title} {clean_body}".strip()),
        identifiers=identifiers(raw),
        accessions=accessions(raw),
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
    # Same submission, stated by EDGAR itself. Checked *before* the veto because the two
    # rules read the same documents and disagree about them: a Form 4 is indexed under the
    # reporting person and under the issuer, so the pair carries one accession number and two
    # different CIKs, and the veto below sees only the CIKs. 3.E found this as "one Form 4
    # clusters twice"; the committed EDGAR fixture has 19 such pairs in 40 entries.
    #
    # A positive rule rather than a loosening of the veto, deliberately. ADR-0009 recorded
    # that the veto "is now load-bearing for a stage that does not exist yet" — 4B's embedding
    # branch has a 14x worse corpus false-merge rate without it, all of it EDGAR — so this
    # adds evidence in front of it instead of weakening it. Equality, not intersection: two
    # filings that merely *cite* a common third document overlap without being one filing,
    # and the Allspring pair pinned below shares a CIK that is also its accession prefix.
    if a.accessions and a.accessions == b.accessions:
        return True

    # A veto, checked before any evidence for merging. Two documents that each carry
    # identifiers and carry *different* ones are different documents, however completely the
    # rest of them agrees. This is what tells 47 filings by one fund trust apart: their titles
    # are byte-identical ("497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)", title overlap
    # 1.000) and the only thing that distinguishes them is the accession number.
    #
    # It fires only when BOTH sides have identifiers, so ordinary prose — which has none — is
    # untouched, and asymmetric evidence (one side has an id, the other doesn't) is not
    # treated as disagreement.
    if a.identifiers and b.identifiers and a.identifiers != b.identifiers:
        return False

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


def authority(domain: str | None) -> float:
    """How much independent reporting a publisher tends to originate.

    `None` is accepted and scores `DEFAULT_AUTHORITY`: `publisher_domain` is nullable, and an
    article whose domain could not be derived is an unknown publisher, which is exactly what
    the default means. Rejecting it would make canonical selection raise on a row the table
    permits.
    """
    return _AUTHORITY.get(domain or "", DEFAULT_AUTHORITY)


# Where an aggregator's submissions actually come from. SPEC §7.4 defines `breadth` as the
# count of **independent** publishers, and an aggregator breaks that definition in a specific
# way: `transform.to_article` sets `publisher_domain` from the submitted URL, so three Hacker
# News posts about one project — its site, its GitHub repo, a thread about it — arrive as
# three distinct domains and score as three independent outlets corroborating a story.
#
# They are one community's attention, not three newsrooms. 3.E recorded it as
# "publisher-diversity inflation" and SPEC §12 carried it into 4A, gating `breadth` and the
# brief's top ten.
#
# Keyed on `source_id` rather than on a domain list because the property belongs to the
# *source*: anything whose documents are user submissions of other people's URLs has this
# shape, and a future aggregator source would need adding here, not to a denylist of domains.
AGGREGATOR_PUBLISHERS: dict[str, str] = {
    "hackernews": "news.ycombinator.com",
}


def effective_publisher(article: Article) -> str:
    """The publisher a cluster should count for `breadth`.

    Identity for ordinary sources — a TechCrunch article's publisher is TechCrunch. For an
    aggregator it is the aggregator itself, collapsing its submissions to one voice however
    many outbound domains they point at. See `AGGREGATOR_PUBLISHERS`.

    Deliberately not applied to `publisher_domain` at parse time: the submitted URL is a true
    fact about the document and `silver.articles` should keep it. This is a *ranking*
    question — what counts as independent corroboration — so it is answered where ranking
    reads, not by rewriting the record (SPEC §6.2).
    """
    aggregated = AGGREGATOR_PUBLISHERS.get(article.get("source_id") or "")
    return aggregated or (article.get("publisher_domain") or "")


def blocking_keys(tokens: frozenset[str], frequency: dict[str, int], threshold: float) -> list[str]:
    """The tokens an article must be indexed under for a Jaccard join to be **exact**.

    Prefix filtering, not LSH. Sort a set by descending global frequency and keep the first
    `|A| - ceil(t*|A|) + 1` tokens: any two sets with Jaccard >= t are then guaranteed to
    share at least one kept token, so blocking on them loses no candidate pair. That is a
    stronger property than banded LSH offers, and it is available here only because the
    decision is a token-overlap threshold rather than a distance.

    Rarest-first is what makes it cheap as well as exact — a bucket keyed on "ai" is most of
    the corpus, one keyed on "unitree" is two articles.

    3.0 measured all-pairs at 3.58M comparisons in 1.3s, so this is not needed for speed
    today; it is what stops the job being quadratic as the lake grows.
    """
    if not tokens:
        return []
    ordered = sorted(tokens, key=lambda t: (-frequency.get(t, 0), t))
    keep = len(ordered) - math.ceil(threshold * len(ordered)) + 1
    return ordered[len(ordered) - max(keep, 1) :]


def exact_dedup[ArticleT: Article](articles: Sequence[ArticleT]) -> tuple[list[ArticleT], int]:
    """Collapse byte-identical reprints. Returns (kept, removed_count).

    Generic in the row type so it returns what it was given: the Spark job hands it articles
    already carrying `prepared` and needs them back, while the in-process path passes plain
    `Article`s. A bare `list[Article]` return would silently widen the former.

    A missing hash is *not* a hash two articles can share. `content_hash` is nullable in
    `ARTICLES_DDL`, and treating null as a value made the first null-hashed article
    swallow every later one — silent data loss dressed as deduplication, and the more
    articles were missing a hash the more of them vanished. `to_article` always computes
    one today, so this has never fired; it is guarded because the column permits it and the
    failure would be invisible in the output.
    """
    seen: set[str] = set()
    kept = []
    for article in articles:
        digest = article["content_hash"]
        if digest is not None:
            if digest in seen:
                continue
            seen.add(digest)
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

    clusters: list[StoryCluster]
    dissolved: int = 0
    dissolved_articles: int = 0


def trusted_timestamp(article: Article) -> datetime:
    """When this article says it happened, if we believe it — else when we saw it.

    SPEC §6.2's rule, applied per article. It used to live only in `ranker.score_cluster`
    and therefore only ever ran against the cluster head, which is why a cluster's age was
    the head's age (see `first_seen` / `last_seen` below).
    """
    published = article.get("published_at")
    if published and not article.get("timestamp_flagged"):
        return ensure_utc(published)
    return ensure_utc(article["fetched_at"])


def group_edges(articles: Sequence[Article], edges: Iterable[tuple[str, str]]) -> ClusterResult:
    """Connected components over a supplied edge list, then stages 3-4. SPEC §7.1.

    Split out from `group_stories` so the in-process path and the Spark job share one
    implementation of everything that happens *after* the pairwise decision — the size
    guard, canonical selection, the publisher count. They differ only in how they arrive at
    the edges: all-pairs here, blocked candidates there. Two copies of this would eventually
    disagree about what a cluster is, and only one of them would be the one being measured.

    Order-independent: connected components do not depend on edge order, and canonical
    selection breaks ties on `(-authority, fetched_at, article_id)`. A replay reproduces a
    run exactly rather than within a tolerance.
    """
    index_of = {article["article_id"]: i for i, article in enumerate(articles)}
    parent = list(range(len(articles)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for left, right in edges:
        union(index_of[left], index_of[right])

    grouped: dict[int, list[Article]] = {}
    for index, article in enumerate(articles):
        grouped.setdefault(find(index), []).append(article)

    # The size guard, before canonical selection: an oversized component has no meaningful
    # head to choose.
    cap = max(MIN_CLUSTER_CAP, int(len(articles) * MAX_CLUSTER_SHARE))
    dissolved = dissolved_articles = 0
    components: list[list[Article]] = []
    for members in grouped.values():
        if len(members) > cap:
            dissolved += 1
            dissolved_articles += len(members)
            components.extend([member] for member in members)
        else:
            components.append(members)

    clusters: list[StoryCluster] = []
    for members in components:
        # Canonical = most authoritative, earliest-seen. The rest become
        # distinct_publisher_count, which feeds ranking instead of being discarded.
        #
        # `effective_publisher`, not `publisher_domain`: see its docstring. Three Show HN
        # posts about one project are one publisher, not three.
        head = min(
            members,
            key=lambda a: (-authority(a["publisher_domain"]), a["fetched_at"], a["article_id"]),
        )
        publishers = {effective_publisher(m) for m in members}
        # Both ends, because they answer different questions and a brief needs the second.
        # The canonical head is by construction the *earliest* article (most authoritative,
        # then earliest seen), so timestamping a cluster by its head measured when the story
        # broke and never when it was last covered — a developing story looked older the
        # longer it ran, which is backwards. Measured on a real window: the Disney/FCC
        # cluster drew coverage across 38 hours and ranked as 65.6h old while its newest
        # article was 23h old (docs/runbooks/phase-3.md 3.B.4).
        references = [trusted_timestamp(m) for m in members]
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
                # When the story broke, and when it was last covered.
                "first_seen": min(references),
                "last_seen": max(references),
                "article_count": len(members),
                "distinct_publisher_count": len(publishers),
                "publishers": sorted(publishers),
                "timestamp_flagged": head["timestamp_flagged"],
                "story_key": head.get("story_key"),
                # Every member, so the Spark job can write the article -> cluster map
                # without re-deriving membership it already computed here.
                "article_ids": sorted(m["article_id"] for m in members),
            }
        )
    return ClusterResult(
        clusters=sorted(clusters, key=lambda c: (-c["distinct_publisher_count"], c["cluster_id"])),
        dissolved=dissolved,
        dissolved_articles=dissolved_articles,
    )


def group_stories(articles: Sequence[Article]) -> ClusterResult:
    """Group articles into story clusters, comparing every pair. SPEC §7.1 stages 2-4.

    O(n^2) and honestly so: 3.0 measured 3.58M comparisons in 1.3 s, which is why the
    in-process path used by `make skeleton` and `signal brief` still runs it whole. The
    Spark job blocks first (`spark/jobs/cluster.py`) and hands its edges to `group_edges`;
    both then follow identical code.
    """
    prepared = [prepare(a["title"], a["body_text"]) for a in articles]
    edges = [
        (articles[i]["article_id"], articles[j]["article_id"])
        for i in range(len(articles))
        for j in range(i + 1, len(articles))
        if decide(prepared[i], prepared[j])
    ]
    return group_edges(articles, edges)


def dedup_ratio(articles_in: int, clusters_out: int) -> float:
    """SPEC §15's headline processing metric."""
    return articles_in / clusters_out if clusters_out else 0.0
