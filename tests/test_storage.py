from __future__ import annotations

from datetime import UTC, datetime

from signal_core.storage import ObjectCache, bronze_partition, write_bronze


def test_bronze_partition_matches_the_s3_key_convention():
    """SPEC §6.4 — local and S3 layouts must be byte-identical, or the skeleton proves nothing."""
    moment = datetime(2026, 8, 18, 7, 30, tzinfo=UTC)
    assert bronze_partition("edgar", moment) == "source=edgar/ingest_date=2026-08-18/hour=07"


def test_write_bronze_round_trips(tmp_path, polled):
    import pyarrow.parquet as pq

    documents, _ = polled
    files = write_bronze(documents, tmp_path)
    assert files

    table = pq.read_table(files[0])
    assert table.num_rows > 0
    assert set(table.column_names) >= {"payload", "content_hash", "fetched_at", "outcome"}


def test_write_bronze_does_not_overwrite_on_replay(tmp_path, polled):
    """SPEC §6.2: a replay lands beside the original, never on top of it."""
    documents, _ = polled
    first = write_bronze(documents, tmp_path)
    replayed = [d.model_copy(update={"ingest_id": d.ingest_id + "-replay"}) for d in documents]
    second = write_bronze(replayed, tmp_path)

    assert set(first).isdisjoint(second)
    assert all(p.exists() for p in first + second)


def test_object_cache_downloads_once(tmp_path):
    """SPEC §10.1: each re-read of a bronze object is billed egress."""
    source = tmp_path / "obj.parquet"
    source.write_bytes(b"payload")
    cache = ObjectCache(tmp_path / "cache")

    assert cache.get("deadbeefcafe") is None
    cache.put("deadbeefcafe", source)
    assert cache.get("deadbeefcafe") is not None
    assert cache.hits == 1 and cache.misses == 1
    assert cache.hit_rate == 0.5
