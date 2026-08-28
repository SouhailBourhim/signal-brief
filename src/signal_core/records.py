"""The row shapes that flow between stages, as types rather than as `dict[str, Any]`.

`contracts.py` types the *edge* of the system — `RawDocument`, `State`, `SourceConfig` are
Pydantic and validated at runtime, because that is where bytes arrive from somewhere that
does not share our assumptions. Everything downstream of parse was untyped: an article, a
cluster and a table row were all `dict[str, Any]`, so `a["publsher_domain"]` was a runtime
`KeyError` in a Spark job rather than a type error in an editor.

These are `TypedDict`s and not Pydantic models, deliberately. Spark hands back
`Row.asDict()`, the jobs pass plain dicts to `createDataFrame`, and the in-process path
shares those same dicts with the eval harness — a model would mean constructing and
unpacking at every one of those seams, at a cost paid per row. A `TypedDict` **is** a dict
at runtime and costs nothing; it exists entirely for mypy, which is exactly the trade
wanted here.

## Why the DDL is parsed rather than restated

The failure this module is built against is 3.D's first defect: *"the deployed table had 17
columns and the DDL had 19"*. `ensure_columns` fixed the deployed-table half of that. The
other half — a writer whose keys have drifted from the DDL it writes into — was still only
caught by running the job.

So a table's columns are **derived** from its DDL by `columns()`, never restated, and
`tests/test_records.py` asserts each `TypedDict` here agrees with its DDL exactly. A column
added to a DDL without the writer being taught to produce it now fails in CI. `normalize.py`
had already hand-copied one such list under a comment promising it "matches `ARTICLES_DDL`'s
column order exactly"; that list is now computed from the DDL it was promising to match.

## What typing this surfaced

Three latent nullability faults, all in code that had passed every test, and all of the same
shape — a column the DDL declares nullable, read as though it never is:

  - `exact_dedup` treated a null `content_hash` as a value, so the first null-hashed article
    swallowed every later one as a "byte-identical reprint". Silent data loss that scaled
    with how many rows lacked a hash.
  - `_to_signed_i64` did arithmetic on a null `simhash`, which would fail a whole window.
  - `authority()` and `prepare()` declared `str` while their bodies already coerced `None`,
    so the signature and the behaviour disagreed about which caller was responsible.

None were reachable today, because `to_article` fills all three. They are recorded because
the class is the point: a nullable column read as non-null is invisible to a test suite whose
fixtures always populate it.

## What is still untyped, and why it stopped here

The brief layer — `read.py`, `select.py`, `ranker.py`, `render.py`, `items.py` — still passes
`dict[str, Any]`. Its rows are a *third* shape: a cluster read back from Athena, joined to
its head's body text, then augmented by the ranker with `score`, `score_components`, `rank`
and `included`. Typing it properly means naming that shape and the enrichment shapes beside
it, which is a second change of this size and not one to bundle in behind a review fix.

`rank` and `score_cluster` take `Mapping[str, Any]` rather than a row type for that reason:
they read a mapping and mutate nothing, so that signature is honest about today rather than
aspirational about tomorrow.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    # `dedup` imports this module for `Article`, so importing it back at runtime would be a
    # cycle. Only `PreparedArticle`'s annotation needs the name, and `from __future__ import
    # annotations` keeps every annotation a string — so the cost of this is that
    # `get_type_hints(PreparedArticle)` needs `dedup` imported to resolve, which the parity
    # test does not ask for and `tests/test_records.py` documents.
    from signal_core.dedup import Prepared

# A DDL line is `name type[ NOT NULL],`. The name is the first token; everything after it is
# the type, which may itself contain commas (`array<string>`, `decimal(10,2)`), so lines are
# split on newlines rather than on commas.
_COLUMN = re.compile(r"^\s*([a-z_][a-z0-9_]*)\s+\S", re.IGNORECASE)


def columns(ddl: str) -> tuple[str, ...]:
    """The column names of a `CREATE TABLE` body, in declaration order.

    Order matters to more than tidiness: `normalize.py`'s `MERGE ... INSERT *` lines columns
    up **positionally**, so a tuple that reordered them would corrupt a table rather than
    fail. Derived from the DDL for that reason — a hand-maintained copy is one careless edit
    away from silently writing `title` into `body_text`.
    """
    found = tuple(match.group(1) for line in ddl.splitlines() if (match := _COLUMN.match(line)))
    if not found:
        raise ValueError(f"no columns parsed from DDL: {ddl[:80]!r}")
    return found


class Article(TypedDict):
    """One row of `silver.articles` — the unit everything after normalize operates on.

    `total=True`: every key is required, because every producer of an article builds it in
    one place (`transform.to_article`) and every consumer reads it having come from the
    table. An optional key here would only move the `None` check to every read site.
    """

    article_id: str
    source_id: str
    url_canonical: str | None
    title: str | None
    body_text: str | None
    published_at: datetime | None
    fetched_at: datetime
    event_date: datetime
    lang: str | None
    publisher_domain: str | None
    authority_score: float | None
    simhash: int | None
    content_hash: str | None
    timestamp_flagged: bool | None
    story_key: str | None
    parse_error: str | None
    external_id: str | None


class PreparedArticle(Article):
    """An `Article` carrying the tokenization `dedup.decide` reads.

    A separate type rather than a `NotRequired` key on `Article`, because `prepared` is not
    a column: `Article` is checked against `ARTICLES_DDL` field-for-field, and a key that no
    table has would have to be excused from that check — weakening the one property the
    parity test exists to hold.

    The Spark job computes it once per article because clustering compares O(n^2) pairs;
    the in-process path passes plain `Article`s and lets `group_stories` prepare them. Both
    reach `group_edges`, which takes the base type.
    """

    prepared: Prepared


class StoryCluster(TypedDict):
    """What `dedup.group_edges` returns: a cluster as the *decision layer* knows it.

    Deliberately not the same shape as `ClusterRow` below, and the difference is the point.
    This carries `body_text` and `article_ids`, which the brief and the map table need and
    `silver.story_clusters` does not store; it does not carry `window_start`, `ordering_key`
    or `algo_version`, which are facts about the *run* that produced the cluster rather than
    about the cluster. Collapsing the two would put run metadata in the in-process path,
    which has no run.
    """

    cluster_id: str
    canonical_article_id: str
    title: str | None
    body_text: str | None
    url_canonical: str | None
    publisher_domain: str | None
    published_at: datetime | None
    fetched_at: datetime
    first_seen: datetime
    last_seen: datetime
    article_count: int
    distinct_publisher_count: int
    publishers: list[str]
    timestamp_flagged: bool | None
    story_key: str | None
    article_ids: list[str]


class ClusterRow(TypedDict):
    """One row of `silver.story_clusters`. Must match `cluster.CLUSTERS_DDL` exactly.

    This is the type 3.D's first defect would have been a compile error in: `first_seen` and
    `last_seen` were added to the DDL, and nothing tied the writer to it.
    """

    cluster_id: str
    window_start: datetime
    window_end: datetime
    canonical_article_id: str
    title: str | None
    url_canonical: str | None
    publisher_domain: str | None
    published_at: datetime | None
    fetched_at: datetime
    first_seen: datetime
    last_seen: datetime
    event_date: datetime
    article_count: int
    distinct_publisher_count: int
    publishers: list[str]
    timestamp_flagged: bool | None
    ordering_key: str
    algo_version: str
    clustered_at: datetime


class ArticleClusterRow(TypedDict):
    """One row of `silver.article_clusters`, the article -> cluster map."""

    article_id: str
    cluster_id: str
    window_start: datetime
    is_canonical: bool
    algo_version: str


def as_article(row: dict[str, Any]) -> Article:
    """Narrow a Spark `Row.asDict()` to `Article`.

    A cast with a name, not a validation: Spark has already enforced the schema on read, so
    re-checking every field per row would buy nothing and cost a pass over the window. What
    it buys is a single, greppable place where the untyped boundary is crossed — the same
    role `staging._s3` plays for boto3.
    """
    return row  # type: ignore[return-value]
