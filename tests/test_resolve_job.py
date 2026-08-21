"""`spark/jobs/resolve.py`. SPEC §7.2, §9; docs/runbooks/phase-3.md 3.C.

The properties worth pinning here are the ones `make eval` cannot see, because they are about
tables rather than about decisions: that re-resolving replaces instead of accumulating, that
a rename supersedes a dimension row instead of overwriting it, and that a load of an
unchanged snapshot is a no-op.

Accuracy is not tested here. `evals/score.py` owns that, against hand labels, and a second
accuracy assertion written from the same head that wrote the resolver would be self-agreement
dressed as verification.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signal_core.entities import dictionary as dict_module
from signal_core.entities.dictionary import Entity

pytestmark = pytest.mark.spark

MENTIONS_TABLE = "silver.entity_mentions"
ENTITIES_TABLE = "silver.dim_entities"
ARTICLES_TABLE = "silver.articles"

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
BUILT_AT = "2026-08-21T00:00:00+00:00"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-resolve", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_tables(spark):
    for table in (MENTIONS_TABLE, ENTITIES_TABLE, ARTICLES_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    yield


def _dictionary(tmp_path, entities, *, built_at=BUILT_AT):
    """A snapshot on disk, because that is what the job loads."""
    path = tmp_path / "dictionary.json.gz"
    dict_module.write(dict_module.build(entities, built_at=built_at, common_words=[]), path)
    dict_module.load.cache_clear()
    return path


def _articles(spark, rows):
    from signal_core.spark.jobs.normalize import ARTICLES_DDL

    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {ARTICLES_TABLE} ({ARTICLES_DDL}) "
        "USING iceberg PARTITIONED BY (days(event_date))"
    )
    columns = [
        line.strip().split()[0] for line in ARTICLES_DDL.strip().splitlines() if line.strip()
    ]
    blank = dict.fromkeys(columns)
    frame = spark.createDataFrame(
        [{**blank, **row} for row in rows],
        schema=", ".join(
            line.strip().removesuffix(",").replace(" NOT NULL", "")
            for line in ARTICLES_DDL.strip().splitlines()
            if line.strip()
        ),
    ).select(*columns)
    frame.writeTo(ARTICLES_TABLE).append()


def _article(article_id: str, title: str, body: str = "", *, event_date=NOW) -> dict:
    return {
        "article_id": article_id,
        "source_id": "rss_tech",
        "title": title,
        "body_text": body,
        "event_date": event_date,
        "fetched_at": event_date,
        "publisher_domain": "example.com",
        "content_hash": article_id,
        "simhash": 0,
        "timestamp_flagged": False,
    }


SEC = [
    Entity("CMCSA", "COMCAST CORP", "public", "sec", ticker="CMCSA", cik="0000902739", rank=194),
    Entity("QTRX", "Quanterix Corp", "public", "sec", ticker="QTRX", cik="0001503274", rank=2000),
]


# --- entity_mentions ------------------------------------------------------------------


def test_a_window_resolves_and_writes_mentions(spark, tmp_path):
    from signal_core.spark.jobs.resolve import resolve_window

    _articles(spark, [_article("a1", "Comcast raises fibre prices")])
    result = resolve_window(
        spark,
        NOW - timedelta(hours=1),
        NOW + timedelta(hours=1),
        dictionary_path=_dictionary(tmp_path, SEC),
    )

    assert result.articles_in == 1
    assert result.mentions_detected > 0
    rows = {r["surface_form"]: r for r in spark.table(MENTIONS_TABLE).collect()}
    assert rows["Comcast"]["entity_id"] == "CMCSA"
    assert rows["Comcast"]["resolution_method"] == "name"
    assert rows["Comcast"]["dictionary_built_at"] == BUILT_AT


def test_an_unlinked_mention_is_stored_with_its_reason(spark, tmp_path):
    """Abstention is an answer, so it gets a row. A table that only held links could not
    distinguish "nothing to resolve" from "the resolver stopped working"."""
    from signal_core.spark.jobs.resolve import resolve_window

    _articles(spark, [_article("a1", "Curvature Beziers Explained Simply")])
    result = resolve_window(
        spark,
        NOW - timedelta(hours=1),
        NOW + timedelta(hours=1),
        dictionary_path=_dictionary(tmp_path, SEC),
    )

    assert result.mentions_linked == 0
    assert result.by_reason, "an unlinked mention records why"
    unlinked = spark.table(MENTIONS_TABLE).where("entity_id IS NULL").collect()
    assert unlinked and all(row["unlinked_reason"] for row in unlinked)


def test_re_resolving_replaces_rather_than_accumulates(spark, tmp_path):
    """A mention is a function of (article, dictionary, algorithm), not a fact. Running twice
    must converge, or every count over this table drifts with the number of re-runs."""
    from signal_core.spark.jobs.resolve import resolve_window

    _articles(spark, [_article("a1", "Comcast raises fibre prices")])
    path = _dictionary(tmp_path, SEC)
    since, until = NOW - timedelta(hours=1), NOW + timedelta(hours=1)

    first = resolve_window(spark, since, until, dictionary_path=path)
    before = spark.table(MENTIONS_TABLE).count()
    second = resolve_window(spark, since, until, dictionary_path=path)

    assert spark.table(MENTIONS_TABLE).count() == before
    assert first.mentions_detected == second.mentions_detected


def test_a_rebuilt_dictionary_replaces_the_old_answers(spark, tmp_path):
    """The case that makes replacement non-negotiable: the same span, a different answer."""
    from signal_core.spark.jobs.resolve import resolve_window

    _articles(spark, [_article("a1", "Quanterix Corp reports results")])
    since, until = NOW - timedelta(hours=1), NOW + timedelta(hours=1)

    resolve_window(spark, since, until, dictionary_path=_dictionary(tmp_path, SEC))
    assert spark.table(MENTIONS_TABLE).where("entity_id = 'QTRX'").count() >= 1

    thinner = _dictionary(tmp_path / "v2", [SEC[0]], built_at="2026-08-22T00:00:00+00:00")
    resolve_window(spark, since, until, dictionary_path=thinner)

    assert spark.table(MENTIONS_TABLE).where("entity_id = 'QTRX'").count() == 0
    assert {r["dictionary_built_at"] for r in spark.table(MENTIONS_TABLE).collect()} == {
        "2026-08-22T00:00:00+00:00"
    }


# --- dim_entities, SCD2 ---------------------------------------------------------------


def test_a_first_load_opens_an_interval_for_every_entity(spark, tmp_path):
    from signal_core.spark.jobs.resolve import VALID_TO_OPEN, load_entities

    result = load_entities(spark, dictionary_path=_dictionary(tmp_path, SEC))

    assert (result.inserted, result.superseded) == (2, 0)
    rows = spark.table(ENTITIES_TABLE).collect()
    assert all(row["is_current"] for row in rows)
    assert all(row["valid_to"].replace(tzinfo=None) == VALID_TO_OPEN for row in rows)
    assert {row["valid_from"].strftime("%Y-%m-%d") for row in rows} == {"2026-08-21"}


def test_loading_the_same_snapshot_twice_supersedes_nothing(spark, tmp_path):
    """Idempotence is what makes `superseded` mean something when it is not zero."""
    from signal_core.spark.jobs.resolve import load_entities

    path = _dictionary(tmp_path, SEC)
    load_entities(spark, dictionary_path=path)
    second = load_entities(spark, dictionary_path=path)

    assert (second.inserted, second.superseded, second.unchanged) == (0, 0, 2)
    assert spark.table(ENTITIES_TABLE).count() == 2


def test_a_rename_supersedes_the_old_row_instead_of_overwriting_it(spark, tmp_path):
    """SPEC §7.2's actual example. An article published before the rename is not
    retroactively about the new name, and the labeled set is labeled that way — so the
    dimension has to be able to answer "who was this, then"."""
    from signal_core.spark.jobs.resolve import load_entities

    load_entities(spark, dictionary_path=_dictionary(tmp_path, SEC))

    renamed = [
        Entity(
            "CMCSA",
            "Comcast Holdings Corp",
            "public",
            "sec",
            ticker="CMCSA",
            cik="0000902739",
            rank=194,
        ),
        SEC[1],
    ]
    result = load_entities(
        spark,
        dictionary_path=_dictionary(tmp_path / "v2", renamed, built_at="2026-09-01T00:00:00+00:00"),
    )

    assert (result.superseded, result.inserted, result.unchanged) == (1, 1, 1)
    history = sorted(
        spark.table(ENTITIES_TABLE).where("entity_id = 'CMCSA'").collect(),
        key=lambda r: r["valid_from"],
    )
    assert [row["canonical_name"] for row in history] == ["COMCAST CORP", "Comcast Holdings Corp"]
    assert [row["is_current"] for row in history] == [False, True]
    # No gap and no overlap: the outgoing interval closes exactly where the new one opens.
    assert history[0]["valid_to"] == history[1]["valid_from"]


def test_an_intervals_history_answers_who_was_this_then(spark, tmp_path):
    """The query the SCD2 shape exists to make possible, run as a query."""
    from signal_core.spark.jobs.resolve import load_entities

    load_entities(spark, dictionary_path=_dictionary(tmp_path, SEC))
    renamed = [
        Entity(
            "CMCSA",
            "Comcast Holdings Corp",
            "public",
            "sec",
            ticker="CMCSA",
            cik="0000902739",
            rank=194,
        ),
        SEC[1],
    ]
    load_entities(
        spark,
        dictionary_path=_dictionary(tmp_path / "v2", renamed, built_at="2026-09-01T00:00:00+00:00"),
    )

    as_of = spark.sql(
        f"SELECT canonical_name FROM {ENTITIES_TABLE} WHERE entity_id = 'CMCSA' "
        "AND valid_from <= TIMESTAMP '2026-08-25 00:00:00' "
        "AND TIMESTAMP '2026-08-25 00:00:00' < valid_to"
    ).collect()
    assert [row["canonical_name"] for row in as_of] == ["COMCAST CORP"]


def test_a_reordered_ticker_file_is_not_a_rename(spark, tmp_path):
    """SEC reorders its file constantly and Wikidata gains aliases weekly. Treating either as
    a rename would fill the dimension with history that records nothing about the world."""
    from signal_core.spark.jobs.resolve import load_entities

    load_entities(spark, dictionary_path=_dictionary(tmp_path, SEC))
    reordered = [
        Entity(
            "CMCSA",
            "COMCAST CORP",
            "public",
            "sec",
            ticker="CMCSA",
            cik="0000902739",
            rank=7,
            aliases=("Comcast Cable",),
        ),
        SEC[1],
    ]
    result = load_entities(
        spark,
        dictionary_path=_dictionary(
            tmp_path / "v2", reordered, built_at="2026-09-01T00:00:00+00:00"
        ),
    )

    assert (result.superseded, result.inserted) == (0, 0)
