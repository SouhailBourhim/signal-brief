"""The typed row shapes, and their parity with the DDLs they claim to describe.

3.D's first defect was *"the deployed table had 17 columns and the DDL had 19"* — found by
the first real run dying on `COLUMN_NOT_FOUND`, after every test passed. `ensure_columns`
closed the deployed-table half of that: a table now grows to match its DDL on every run.

This file closes the other half. A DDL is the schema; a `TypedDict` in `records.py` is what
the Python that writes into it believes the schema is, and nothing tied the two together —
so a column added to a DDL without the writer being taught to produce it stayed invisible
until a job ran against real data.

These are cheap, dependency-free, and run in the non-Spark half of the suite, which matters:
the drift they catch is introduced while editing a DDL, and that is when they should fail.
"""

from __future__ import annotations

import typing

import pytest

from signal_core.records import (
    Article,
    ArticleClusterRow,
    ClusterRow,
    columns,
)
from signal_core.spark.jobs.cluster import ARTICLE_CLUSTERS_DDL, CLUSTERS_DDL
from signal_core.spark.jobs.normalize import ARTICLES_DDL

# Each row type and the DDL it must agree with, exactly and in order.
TABLES = [
    ("silver.articles", Article, ARTICLES_DDL),
    ("silver.story_clusters", ClusterRow, CLUSTERS_DDL),
    ("silver.article_clusters", ArticleClusterRow, ARTICLE_CLUSTERS_DDL),
]


@pytest.mark.parametrize(("table", "record", "ddl"), TABLES, ids=[t[0] for t in TABLES])
def test_the_row_type_matches_its_ddl(table, record, ddl):
    """The 3.D defect, as a test.

    Order as well as membership, because `normalize.py`'s `MERGE ... INSERT *` lines columns
    up positionally — a reordering writes the right number of values into the wrong columns,
    which is worse than failing.
    """
    declared = tuple(typing.get_type_hints(record))
    assert declared == columns(ddl), (
        f"{table}: the TypedDict in records.py and the DDL disagree. "
        f"Adding a column means teaching both — that is the point of this test."
    )


def test_columns_parses_types_that_contain_commas():
    """`array<string>` and `decimal(10,2)` are why the parser splits on newlines rather than
    on commas. `publishers array<string>` in CLUSTERS_DDL is the live case."""
    assert columns("""
        a string NOT NULL,
        b array<string>,
        c decimal(10,2),
        d map<string, int>
    """) == ("a", "b", "c", "d")


def test_columns_refuses_a_ddl_it_understood_nothing_of():
    """Returning `()` from a bad parse would make every parity test above pass vacuously —
    the failure mode a regex-based reader has to be loud about."""
    with pytest.raises(ValueError, match="no columns parsed"):
        columns("-- just a comment\n")


def test_the_articles_column_list_is_derived_rather_than_restated():
    """`normalize.py` used to carry a hand-copied `_ARTICLES_COLUMNS` under a comment
    promising it "matches ARTICLES_DDL's column order exactly". The promise was true and
    unenforced; this asserts it is now computed from the thing it promises to match."""
    from signal_core.spark.jobs.normalize import _ARTICLES_COLUMNS

    assert tuple(_ARTICLES_COLUMNS) == columns(ARTICLES_DDL)


# --- the nullability the types made visible -------------------------------------------


def test_a_null_content_hash_is_not_a_hash_two_articles_share():
    """`content_hash` is nullable in `ARTICLES_DDL`, and `exact_dedup` treated null as a
    value: the first null-hashed article was kept and every later one dropped as a
    byte-identical reprint. Silent data loss that scaled with how many rows were missing a
    hash, and invisible in the output — the count of "exact duplicates removed" looked
    healthy.

    Unreachable today because `to_article` always computes a hash. Pinned because the column
    permits it, and because typing `Article` against the DDL is what surfaced it.
    """
    from signal_core.dedup import exact_dedup

    articles = [
        {"article_id": "a", "content_hash": None},
        {"article_id": "b", "content_hash": None},
        {"article_id": "c", "content_hash": "x"},
        {"article_id": "d", "content_hash": "x"},
    ]
    kept, removed = exact_dedup(articles)  # type: ignore[arg-type]

    assert [a["article_id"] for a in kept] == ["a", "b", "c"]
    assert removed == 1, "only the genuine duplicate is a duplicate"


def test_a_null_simhash_survives_the_signed_cast():
    """`simhash` is nullable too, and `_to_signed_i64` did arithmetic on it unguarded — a
    null would have raised `TypeError` and failed the whole window rather than staying null."""
    from signal_core.spark.jobs.normalize import _to_signed_i64

    assert _to_signed_i64(None) is None
    assert _to_signed_i64(1) == 1
    assert _to_signed_i64(2**63) == -(2**63), "the wrap this function exists for still works"
