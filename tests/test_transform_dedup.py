from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from signal_core.dedup import (
    SAME_STORY_JACCARD,
    content_tokens,
    dedup_ratio,
    exact_dedup,
    group_stories,
    jaccard,
)
from signal_core.parse import get_parser
from signal_core.transform import canonical_url, publisher_domain, to_article


def _bronze(payload: dict, fetched_at: datetime | None = None) -> dict:
    return {
        "source_id": "fake",
        "fetched_at": fetched_at or datetime.now(UTC),
        "content_hash": "raw",
        "payload": json.dumps(payload).encode("utf-8"),
    }


def _articles(bronze_row: dict) -> list[dict]:
    """The two 2.B steps chained: bytes -> `ParsedItem`s -> silver rows."""
    result = get_parser(bronze_row["source_id"])(bronze_row["payload"])
    return [to_article(item, bronze_row) for item in result.items]


def _article(bronze_row: dict) -> dict:
    (row,) = _articles(bronze_row)
    return row


def test_canonical_url_strips_tracking_keeps_meaning():
    assert canonical_url("https://Example.com/a/?utm_source=x&id=7") == "https://example.com/a?id=7"


def test_publisher_domain_drops_www():
    assert publisher_domain("https://www.reuters.com/x") == "reuters.com"


def test_normalize_quarantines_instead_of_raising():
    """SPEC §6.2: failed records are quarantined with a reason, never dropped."""
    bad = {
        "source_id": "fake",
        "fetched_at": datetime.now(UTC),
        "content_hash": "raw",
        "payload": b"not json at all",
    }
    result = get_parser("fake")(bad["payload"])
    assert result.error is not None and result.error.startswith("payload_not_json")

    row = _article(_bronze({"title": "", "url": ""}))
    assert row["parse_error"] == "missing_title_or_url"


def test_normalize_flags_missing_timestamp():
    row = _article(_bronze({"title": "T", "body": "B", "url": "https://a.com/x"}))
    assert row["parse_error"] is None
    assert row["timestamp_flagged"] is True


def test_event_date_falls_back_to_fetched_at_when_published_at_is_missing():
    """ADR-0007: a null `published_at` can't be pruned, so `event_date` coalesces to
    `fetched_at`, which is never null."""
    now = datetime.now(UTC)
    row = _article(_bronze({"title": "T", "body": "B", "url": "https://a.com/x"}, fetched_at=now))
    assert row["published_at"] is None
    assert row["event_date"] == row["fetched_at"] == now


def test_event_date_is_published_at_when_known():
    now = datetime.now(UTC)
    published = now - timedelta(hours=2)
    row = _article(
        _bronze(
            {
                "title": "T",
                "body": "B",
                "url": "https://a.com/x",
                "published_at": published.isoformat(),
            },
            fetched_at=now,
        )
    )
    assert row["event_date"] == row["published_at"] == published


def test_normalize_accepts_a_credible_timestamp():
    now = datetime.now(UTC)
    row = _article(
        _bronze(
            {
                "title": "T",
                "body": "B",
                "url": "https://a.com/x",
                "published_at": (now - timedelta(hours=2)).isoformat(),
            },
            fetched_at=now,
        )
    )
    assert row["timestamp_flagged"] is False


def test_normalize_survives_a_malformed_timestamp():
    row = _article(
        _bronze({"title": "T", "body": "B", "url": "https://a.com/x", "published_at": "not-a-date"})
    )
    assert row["parse_error"] is None and row["timestamp_flagged"] is True


def test_article_id_is_stable_across_tracking_variants():
    a = _article(_bronze({"title": "T", "body": "B", "url": "https://a.com/x"}))
    b = _article(_bronze({"title": "T", "body": "B", "url": "https://a.com/x?utm_source=n"}))
    assert a["article_id"] == b["article_id"]


def test_exact_dedup_collapses_byte_identical_reprints():
    articles = [{"content_hash": "h1"}, {"content_hash": "h1"}, {"content_hash": "h2"}]
    kept, removed = exact_dedup(articles)
    assert len(kept) == 2 and removed == 1


def _polled_articles(documents) -> list[dict]:
    articles = []
    for d in documents:
        articles.extend(
            _articles(
                {
                    "source_id": d.source_id,
                    "fetched_at": d.fetched_at,
                    "content_hash": d.content_hash,
                    "payload": d.payload,
                }
            )
        )
    return articles


def test_group_stories_collapses_syndication(polled):
    """The headline claim of SPEC §7.1, asserted end to end on the fixture."""
    documents, _ = polled
    articles = [a for a in _polled_articles(documents) if not a["parse_error"]]
    deduped, exact_removed = exact_dedup(articles)
    clusters = group_stories(deduped)

    assert exact_removed >= 1, "fixture must contain a byte-identical reprint"
    assert len(clusters) < len(articles), "syndication must collapse"

    acq = next(c for c in clusters if "Northwind" in c["title"])
    assert acq["distinct_publisher_count"] >= 3, "the four-publisher event must collapse"
    # Canonical head is the most authoritative publisher in the group.
    assert acq["publisher_domain"] in {"arstechnica.com", "techcrunch.com", "theverge.com"}


def test_no_article_is_lost_to_clustering(polled):
    documents, _ = polled
    articles = _polled_articles(documents)
    deduped, _ = exact_dedup([a for a in articles if not a["parse_error"]])
    clusters = group_stories(deduped)
    assert sum(c["article_count"] for c in clusters) == len(deduped)


def test_dedup_ratio_is_safe_at_zero():
    assert dedup_ratio(10, 0) == 0.0
    assert dedup_ratio(10, 5) == 2.0


def test_jaccard_separates_same_story_from_unrelated():
    """The measurement behind SAME_STORY_JACCARD, kept as a test so retuning is deliberate."""
    a = content_tokens(
        "Northwind said Tuesday it would acquire Lumen Robotics in a cash "
        "deal valued at 2.4 billion dollars"
    )
    reworded = content_tokens(
        "Lumen Robotics will be acquired by Northwind for 2.4 billion "
        "dollars in cash, the companies confirmed Tuesday"
    )
    unrelated = content_tokens(
        "Consumer prices rose 0.2 percent in July, below the 0.3 percent economists expected"
    )

    assert jaccard(a, reworded) >= SAME_STORY_JACCARD
    assert jaccard(a, unrelated) < SAME_STORY_JACCARD


def test_jaccard_is_safe_on_empty_input():
    assert jaccard(frozenset(), content_tokens("anything")) == 0.0


def test_unrelated_stories_are_not_merged(polled):
    """Over-merging is the failure that makes a brief useless; assert it does not happen."""
    documents, _ = polled
    articles = _polled_articles(documents)
    deduped, _ = exact_dedup([a for a in articles if not a["parse_error"]])
    clusters = group_stories(deduped)

    # story_key is the fixture's ground truth: no cluster may mix two of them.
    for cluster in clusters:
        members = [a for a in deduped if a["title"] == cluster["title"]]
        assert len({m["story_key"] for m in members}) == 1
    assert len(clusters) >= 5, "distinct events must stay distinct"
