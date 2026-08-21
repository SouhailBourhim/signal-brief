from __future__ import annotations

from datetime import UTC, datetime, timedelta

from signal_core.hashing import content_hash, enrichment_cache_key, hamming, simhash64
from signal_core.timeutil import brief_date, ensure_utc, timestamps_disagree


def test_content_hash_ignores_formatting_but_not_words():
    assert content_hash("Hello  World") == content_hash("hello world")
    assert content_hash("hello world") != content_hash("hello worlds")


def test_simhash_separates_a_light_edit_from_unrelated_text():
    """A property of the hash, independent of what the pipeline does with it.

    A one-word edit lands around 8 bits and an unrelated article far above it, which is what
    makes simhash a meaningful signal at all. Whether the *pipeline* acts on 8 bits is a
    separate decision, pinned below.
    """
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

    assert hamming(a, edited) < hamming(a, unrelated)
    assert hamming(a, edited) <= 12, "a one-word edit is a small distance"
    assert hamming(a, unrelated) > 12, "unrelated text is not"


def test_the_pipeline_acts_on_exact_equality_only():
    """The threshold is 0, and this pins the reason rather than the number.

    3.D measured unrelated real articles colliding inside 10 and 12 bits — 224-token bodies
    with title overlap 0.00 — and transitive closure chained them into a 45-article cluster
    holding Disney/FCC, a Grok exploit and a corgi tracker. Both labeled sets score
    identically at every distance from 0 to 12, so the tolerance bought nothing measurable
    and cost that. See `dedup.NEAR_DUPLICATE_DISTANCE` for the full trail.

    A light edit is therefore NOT caught here; it is caught by the title path, which is what
    3.B measured it to be caught by all along.
    """
    from signal_core.dedup import NEAR_DUPLICATE_DISTANCE

    assert NEAR_DUPLICATE_DISTANCE == 0

    a = simhash64("Northwind said Tuesday it would acquire Lumen Robotics for 2.4 billion")
    edited = simhash64("Northwind said Tuesday it would acquire Lumen Robotics for 2.5 billion")
    assert hamming(a, edited) > NEAR_DUPLICATE_DISTANCE

    # ...and the same edit is still one story, because `decide` reads the titles.
    from signal_core.dedup import is_same_story

    assert is_same_story(
        "Northwind acquires Lumen Robotics in cash deal",
        "",
        "Northwind acquires Lumen Robotics in a cash deal",
        "",
    )


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
