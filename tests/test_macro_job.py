"""`gold.macro_observations` — the bitemporal store. SPEC §8, §9; 4B.I.

The differentiator the README leads with, so the tests are about the two properties that make
it one: every vintage survives, and the derived columns tell the truth about which value is
current and by how much the last one moved.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from signal_core.contracts import FetchOutcome, PayloadFormat, RawDocument
from signal_core.staging import write_staging
from signal_core.timeutil import utc_now

pytestmark = pytest.mark.spark

BRONZE_TABLE = "bronze.raw_documents"
MACRO_TABLE = "gold.macro_observations"

STILL_CURRENT = "9999-12-31"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="Spark tests need pyspark and a JVM")
    from signal_core.spark.session import build_iceberg_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_iceberg_session("signal-test-macro", warehouse=warehouse, catalog="test")
    yield session
    session.stop()


@pytest.fixture
def staging(tmp_path):
    return tmp_path / "staging"


@pytest.fixture(autouse=True)
def clean_tables(spark):
    for table in (BRONZE_TABLE, MACRO_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    yield


def _alfred(observations: list[tuple[str, str, str, str | None]]) -> bytes:
    """`observations` is [(period, realtime_start, realtime_end, value), ...]."""
    rows = [
        {
            "date": period,
            "realtime_start": start,
            "realtime_end": end,
            "value": "." if value is None else value,
        }
        for period, start, end, value in observations
    ]
    return json.dumps(
        {"count": len(rows), "offset": 0, "limit": 100000, "observations": rows}
    ).encode()


def _doc(
    index: int, series_id: str, payload: bytes, *, fetched_at: datetime | None = None
) -> RawDocument:
    return RawDocument(
        ingest_id=f"macro-{index:04d}-{series_id}",
        source_id="macro",
        fetched_at=fetched_at or utc_now(),
        source_url=(
            f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
            "&realtime_start=2015-01-01&realtime_end=9999-12-31&file_type=json"
        ),
        http_status=200,
        outcome=FetchOutcome.OK,
        etag=None,
        last_modified=None,
        content_hash=f"{index:064d}",
        payload=payload,
        payload_format=PayloadFormat.JSON,
        latency_ms=12,
        byte_count=len(payload),
    )


def _commit(spark, staging, docs) -> None:
    from signal_core.spark.jobs.commit_bronze import commit

    write_staging(docs, staging)
    commit(spark, staging, table=BRONZE_TABLE)


def _window(fetched_at: datetime) -> tuple[datetime, datetime]:
    return fetched_at - timedelta(minutes=1), fetched_at + timedelta(minutes=1)


def _rows(spark, series_id: str = "PAYEMS", period: date | None = None):
    df = spark.table(MACRO_TABLE).where(f"series_id = '{series_id}'")
    if period is not None:
        df = df.where(f"period = date '{period.isoformat()}'")
    return sorted(df.collect(), key=lambda r: r.vintage_date)


# --- the two time axes --------------------------------------------------------------------


def test_every_vintage_of_a_revised_period_lands_as_its_own_row(spark, staging):
    """SPEC §8's entire argument. A pipeline that overwrote the first two would destroy the
    fact "payrolls revised down 46k" is made of."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred(
                    [
                        ("2026-05-01", "2026-06-05", "2026-07-02", "159310"),
                        ("2026-05-01", "2026-07-03", "2026-08-06", "159264"),
                        ("2026-05-01", "2026-08-07", STILL_CURRENT, "159218"),
                    ]
                ),
                fetched_at=now,
            )
        ],
    )

    result = macro_window(spark, *_window(now))

    assert result.observations_committed == 3
    assert [r.value for r in _rows(spark)] == [159310.0, 159264.0, 159218.0]


def test_the_series_id_comes_from_the_stored_url_not_the_payload(spark, staging):
    """FRED does not echo it in the body. If this were dropped, six series would merge under
    one empty id — the failure `_extract_row` refuses the row rather than risk."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "1")]),
                fetched_at=now,
            ),
            _doc(
                2,
                "UNRATE",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "2")]),
                fetched_at=now,
            ),
        ],
    )

    result = macro_window(spark, *_window(now))

    assert result.series_seen == 2
    assert {r.series_id for r in spark.table(MACRO_TABLE).collect()} == {"PAYEMS", "UNRATE"}


def test_only_the_newest_vintage_of_a_period_is_latest(spark, staging):
    """The single most damaging thing a bitemporal store can get wrong: two rows claiming to
    be current makes every "what is the number now" query silently double."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred(
                    [
                        ("2026-05-01", "2026-06-05", "2026-07-02", "159310"),
                        ("2026-05-01", "2026-07-03", STILL_CURRENT, "159264"),
                    ]
                ),
                fetched_at=now,
            )
        ],
    )
    macro_window(spark, *_window(now))

    rows = _rows(spark, period=date(2026, 5, 1))
    assert [r.is_latest for r in rows] == [False, True]


def test_the_still_current_sentinel_is_stored_as_null(spark, staging):
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "1")]),
                fetched_at=now,
            )
        ],
    )
    macro_window(spark, *_window(now))

    assert _rows(spark)[0].superseded_at is None


# --- revision_delta -----------------------------------------------------------------------


def test_a_revision_records_the_difference_from_the_previous_vintage(spark, staging):
    """§8's worked example, in miniature: "payrolls revised down 46k". The brief states the
    delta, so the delta is a stored fact rather than something a reader re-derives."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred(
                    [
                        ("2026-06-01", "2026-07-02", "2026-08-06", "159842"),
                        ("2026-06-01", "2026-08-07", STILL_CURRENT, "159796"),
                    ]
                ),
                fetched_at=now,
            )
        ],
    )
    result = macro_window(spark, *_window(now))

    rows = _rows(spark, period=date(2026, 6, 1))
    assert rows[0].revision_delta is None, "a first vintage has nothing to be revised from"
    assert rows[1].revision_delta == pytest.approx(-46.0)
    assert result.revisions_found == 1


def test_a_first_vintage_has_a_null_delta_not_a_zero(spark, staging):
    """ "Not yet revised" and "revised by zero" are different facts about the world, and §8's
    whole argument is that collapsing facts about revisions is how pipelines lose them."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-07-01", "2026-08-07", STILL_CURRENT, "160021")]),
                fetched_at=now,
            )
        ],
    )
    macro_window(spark, *_window(now))

    assert _rows(spark)[0].revision_delta is None


def test_a_revision_to_or_from_a_missing_value_is_null_not_the_whole_value(spark, staging):
    """FRED's `"."` means unpublished. Treating it as zero would report a fictional revision
    the size of the entire series."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred(
                    [
                        ("2026-08-01", "2026-09-04", "2026-10-02", None),
                        ("2026-08-01", "2026-10-03", STILL_CURRENT, "160500"),
                    ]
                ),
                fetched_at=now,
            )
        ],
    )
    macro_window(spark, *_window(now))

    rows = _rows(spark, period=date(2026, 8, 1))
    assert rows[0].value is None
    assert rows[1].revision_delta is None, "160500 is not a revision *from* nothing"


def test_a_later_vintage_demotes_a_row_the_batch_never_touched(spark, staging):
    """Why `recompute_derived` runs over the whole table rather than the incoming batch. The
    May row was committed yesterday and is not in today's window, but today's new vintage
    means it is no longer current."""
    from signal_core.spark.jobs.macro import macro_window

    first_at = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "159310")]),
                fetched_at=first_at,
            )
        ],
    )
    macro_window(spark, *_window(first_at))
    assert [r.is_latest for r in _rows(spark)] == [True]

    later = first_at + timedelta(days=1)
    _commit(
        spark,
        staging,
        [
            _doc(
                2,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-07-03", STILL_CURRENT, "159264")]),
                fetched_at=later,
            )
        ],
    )
    macro_window(spark, *_window(later))

    rows = _rows(spark, period=date(2026, 5, 1))
    assert [r.is_latest for r in rows] == [False, True]
    assert sum(1 for r in rows if r.is_latest) == 1
    assert rows[1].revision_delta == pytest.approx(-46.0)


# --- replay -------------------------------------------------------------------------------


def test_reloading_the_same_window_commits_nothing(spark, staging):
    """SPEC §6.3's replay guarantee, in its easiest form. A published vintage never changes,
    so the natural key is immutable and re-reading it is free of consequence."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "159310")]),
                fetched_at=now,
            )
        ],
    )
    first = macro_window(spark, *_window(now))
    second = macro_window(spark, *_window(now))

    assert first.observations_committed == 1
    assert second.observations_committed == 0
    assert second.table_rows == first.table_rows


def test_an_overlapping_refetch_inside_one_window_collapses_to_one_row(spark, staging):
    """Every fetch re-states the full bounded history, so two polls a day apart carry the
    same vintages. The later `observed_at` is the honest one."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    payload = _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "159310")])
    _commit(
        spark,
        staging,
        [
            _doc(1, "PAYEMS", payload, fetched_at=now - timedelta(seconds=30)),
            _doc(2, "PAYEMS", payload, fetched_at=now),
        ],
    )
    result = macro_window(spark, *_window(now))

    assert result.bronze_rows == 2
    assert result.observations_extracted == 1
    assert result.observations_committed == 1


# --- schema drift -------------------------------------------------------------------------


def test_a_table_created_before_a_column_existed_gains_it(spark):
    """3.D's defect, which recurred in 4A on `ops.maintenance_runs`. `CREATE TABLE IF NOT
    EXISTS` is a no-op against a live table, so this creates the *old* shape first — the way
    a deployed table already exists — rather than fresh from the current DDL."""
    from signal_core.spark.jobs.macro import MACRO_DDL, ensure_table

    older = "\n".join(
        line for line in MACRO_DDL.strip().splitlines() if "superseded_at" not in line
    ).rstrip(",")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")
    spark.sql(f"CREATE TABLE {MACRO_TABLE} ({older}) USING iceberg")
    assert "superseded_at" not in {f.name for f in spark.table(MACRO_TABLE).schema.fields}

    added = ensure_table(spark)

    assert added == ["superseded_at"]
    assert "superseded_at" in {f.name for f in spark.table(MACRO_TABLE).schema.fields}


def test_a_window_with_no_macro_documents_is_not_a_failure(spark, staging):
    """A window containing no macro poll is ordinary — the source runs once a day, so 23 of
    every 24 hourly windows are empty. Bronze itself exists (`commit_bronze` created it);
    scoped this way rather than dropping the table, because a missing `bronze.raw_documents`
    is a different fault entirely and `market_window` does not pretend to survive it either."""
    from signal_core.spark.jobs.macro import macro_window

    now = utc_now()
    _commit(
        spark,
        staging,
        [
            _doc(
                1,
                "PAYEMS",
                _alfred([("2026-05-01", "2026-06-05", STILL_CURRENT, "1")]),
                fetched_at=now,
            )
        ],
    )

    result = macro_window(spark, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC))

    assert result.bronze_rows == 0
    assert result.observations_committed == 0
    assert result.revisions_found == 0
