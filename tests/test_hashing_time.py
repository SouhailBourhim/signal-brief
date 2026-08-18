from __future__ import annotations

from datetime import UTC, datetime, timedelta

from signal_core.hashing import content_hash, enrichment_cache_key, hamming, simhash64
from signal_core.timeutil import brief_date, ensure_utc, timestamps_disagree


def test_content_hash_ignores_formatting_but_not_words():
    assert content_hash("Hello  World") == content_hash("hello world")
    assert content_hash("hello world") != content_hash("hello worlds")


def test_simhash_catches_light_edits():
    """SPEC §7.1 stage 2: reprints and light rewrites, which is all simhash claims."""
    from signal_core.dedup import NEAR_DUPLICATE_DISTANCE

    a = simhash64(
        "Northwind said Tuesday it would acquire Lumen Robotics in a cash deal "
        "valued at 2.4 billion dollars, its largest purchase to date"
    )
    edited = simhash64(
        "Northwind said Tuesday it would acquire Lumen Robotics in a cash deal "
        "worth 2.4 billion dollars, its largest purchase to date"
    )
    unrelated = simhash64(
        "Consumer prices rose 0.2 percent in July, below the 0.3 percent "
        "economists expected, while core inflation held steady"
    )

    assert hamming(a, edited) <= NEAR_DUPLICATE_DISTANCE
    assert hamming(a, unrelated) > NEAR_DUPLICATE_DISTANCE


def test_simhash_does_not_claim_to_see_semantic_rewrites():
    """Guards the layering: if this ever passes, stage 3 has been folded into stage 2 by
    accident and the dedup metrics stop meaning what §7.1 says they mean."""
    from signal_core.dedup import NEAR_DUPLICATE_DISTANCE

    a = simhash64(
        "Northwind said Tuesday it would acquire Lumen Robotics in a cash deal "
        "valued at 2.4 billion dollars"
    )
    reworded = simhash64(
        "Lumen Robotics will be acquired by Northwind for 2.4 billion "
        "dollars in cash, the companies confirmed Tuesday"
    )
    assert hamming(a, reworded) > NEAR_DUPLICATE_DISTANCE


def test_simhash_identical_text_is_distance_zero():
    text = "Perihelion Energy filed its S-1 with the SEC on Monday"
    assert hamming(simhash64(text), simhash64(text)) == 0


def test_simhash_handles_short_and_empty_text():
    assert simhash64("") == 0
    assert isinstance(simhash64("two words"), int)


def test_cache_key_changes_with_every_component():
    """SPEC §7.3: prompt or model changes must invalidate, or cache-hit rate is a lie."""
    base = enrichment_cache_key("text", "sha256:aaa", "v1")
    assert base != enrichment_cache_key("other", "sha256:aaa", "v1")
    assert base != enrichment_cache_key("text", "sha256:bbb", "v1")
    assert base != enrichment_cache_key("text", "sha256:aaa", "v2")
    assert base == enrichment_cache_key("text", "sha256:aaa", "v1")


def test_missing_published_at_disagrees():
    assert timestamps_disagree(datetime.now(UTC), None)


def test_future_published_at_disagrees():
    """We cannot have fetched an article before it was published."""
    now = datetime.now(UTC)
    assert timestamps_disagree(now, now + timedelta(hours=2))


def test_small_clock_skew_is_tolerated():
    now = datetime.now(UTC)
    assert not timestamps_disagree(now, now + timedelta(minutes=2))


def test_ancient_published_at_disagrees():
    now = datetime.now(UTC)
    assert timestamps_disagree(now, now - timedelta(days=10))
    assert not timestamps_disagree(now, now - timedelta(hours=6))


def test_ensure_utc_does_not_guess_local_zone():
    naive = datetime(2026, 3, 14, 12, 0)
    assert ensure_utc(naive).tzinfo is UTC
    assert ensure_utc(naive).hour == 12


def test_brief_date_uses_reader_timezone():
    """A 00:30 Casablanca edition must not be labelled with the previous UTC day."""
    moment = datetime(2026, 6, 1, 23, 30, tzinfo=UTC)  # 00:30 next day in Casablanca (UTC+1)
    assert brief_date(moment) == "2026-06-02"
