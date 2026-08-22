from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from signal_core.dedup import (
    BODY_JACCARD,
    TITLE_JACCARD,
    content_tokens,
    dedup_ratio,
    exact_dedup,
    group_stories,
    is_same_story,
    jaccard,
    strip_boilerplate,
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
    clusters = group_stories(deduped).clusters

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
    clusters = group_stories(deduped).clusters
    assert sum(c["article_count"] for c in clusters) == len(deduped)


def test_dedup_ratio_is_safe_at_zero():
    assert dedup_ratio(10, 0) == 0.0
    assert dedup_ratio(10, 5) == 2.0


def test_the_decision_separates_same_story_from_unrelated():
    """The capability behind the thresholds, kept as a test so retuning stays deliberate.

    Asserts `is_same_story` rather than a raw Jaccard: the thresholds were fitted in 3.B
    (`evals/fit_thresholds.py`) and a test on one of them in isolation would go green while
    the decision they combine into regressed.
    """
    acquisition = (
        "Northwind acquires Lumen Robotics",
        "Northwind said Tuesday it would acquire Lumen Robotics in a cash "
        "deal valued at 2.4 billion dollars",
    )
    reworded = (
        "Lumen Robotics to be bought by Northwind",
        "Lumen Robotics will be acquired by Northwind for 2.4 billion "
        "dollars in cash, the companies confirmed Tuesday",
    )
    unrelated = (
        "Central bank holds rates steady",
        "Consumer prices rose 0.2 percent in July, below the 0.3 percent economists expected",
    )

    assert is_same_story(*acquisition, *reworded)
    assert not is_same_story(*acquisition, *unrelated)


def test_boilerplate_stripping_removes_markup_not_prose():
    """SPEC §7.1 stage 1. Phase 0 never implemented this, and 82% of random EDGAR pairs
    cleared the same-story threshold on the residue (docs/runbooks/phase-3.md 3.0)."""
    stripped = strip_boilerplate(
        '<figure><img alt="a caption nobody wrote" data-portal-copyright="x"/></figure>'
        "<p>Egypt&#x27;s Theban necropolis holds over 400 tombs.</p> https://example.com/x"
    )
    assert "Theban necropolis holds over 400 tombs" in stripped
    assert "img" not in stripped and "data-portal-copyright" not in stripped
    assert "example.com" not in stripped
    assert "Egypt's" in stripped, "entities must be unescaped, not dropped"


def test_edgar_filings_do_not_merge_on_their_own_field_names():
    """The precision failure 3.B fixes, as a regression test. Two unrelated filings share
    `Filed`/`AccNo`/`Size` and a filing date, and nothing else."""
    a = (
        "6-K - Haleon plc (0001900304) (Filer)",
        "<b>Filed:</b> 2026-08-19 <b>AccNo:</b> 0001654954-26-007739 <b>Size:</b> 271 KB",
    )
    b = (
        "6-K - Marti Technologies, Inc. (0001852767) (Filer)",
        "<b>Filed:</b> 2026-08-19 <b>AccNo:</b> 0001213900-26-091421 <b>Size:</b> 185 KB",
    )
    assert not is_same_story(*a, *b)


def test_a_headline_against_a_long_body_still_matches():
    """The recall failure 3.B fixes. One source carries a headline and no body, another 120
    tokens of prose; pooling them scored 0.041 against a title overlap of 0.833."""
    ap = ("NASA calls off Swift rescue mission", "")
    ars = (
        "NASA calls off mission to rescue Swift gamma-ray observatory",
        "<p>" + "The agency said the observatory could not be reached in time. " * 20 + "</p>",
    )
    assert is_same_story(*ap, *ars)


def test_two_filings_by_one_company_are_two_stories():
    """A fund trust lodging 47 supplements in a day is 47 filings, not one story. Their
    titles are byte-identical — title overlap 1.000 — so only the accession number tells
    them apart, which is why `prepare` keeps identifiers instead of discarding them."""
    a = (
        "497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)",
        "<b>Filed:</b> 2026-08-20 <b>AccNo:</b> 0001081400-26-000347 <b>Size:</b> 46 KB",
    )
    b = (
        "497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)",
        "<b>Filed:</b> 2026-08-20 <b>AccNo:</b> 0001081400-26-000352 <b>Size:</b> 46 KB",
    )
    assert not is_same_story(*a, *b)
    # The same filing, fetched twice, still merges: identical identifiers are agreement.
    assert is_same_story(*a, *a)


def test_one_form_4_indexed_under_two_ciks_is_one_story():
    """3.E's "EDGAR shaping": EDGAR indexes a submission under every CIK it concerns, so a
    Form 4 arrives twice — once under the reporting person, once under the issuer. The
    titles name different parties and the CIKs differ, so the identity veto sees two
    documents. The accession number says otherwise, and it is EDGAR's own statement that
    this is one filing.

    Real entries from the committed feed, which holds 19 such pairs in 40 entries."""
    reporting = (
        "4 - Koss Jennifer G. (0001872100) (Reporting)",
        "<b>Filed:</b> 2026-08-21 <b>AccNo:</b> 0001872100-26-000003 <b>Size:</b> 9 KB",
    )
    issuer = (
        "4 - Reservoir Media, Inc. (0001824403) (Issuer)",
        "<b>Filed:</b> 2026-08-21 <b>AccNo:</b> 0001872100-26-000003 <b>Size:</b> 9 KB",
    )
    assert is_same_story(*reporting, *issuer)


def test_the_accession_rule_needs_the_whole_number_not_a_shared_prefix():
    """Why the rule reads `accessions` and not `identifiers`. Tokenization splits
    `0001081400-26-000347` on the hyphen, and the leading run is the filer's own CIK — so
    the two Allspring filings above already *share* two of three fragments. Equality on the
    whole accession separates them; any intersection rule over the fragments would not."""
    from signal_core.dedup import accessions, identifiers, prepare

    a = prepare(
        "497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)",
        "<b>AccNo:</b> 0001081400-26-000347",
    )
    b = prepare(
        "497 - ALLSPRING FUNDS TRUST (0001081400) (Filer)",
        "<b>AccNo:</b> 0001081400-26-000352",
    )
    assert a.identifiers & b.identifiers, "the fragments overlap, which is the trap"
    assert a.accessions != b.accessions, "the whole numbers do not"

    assert accessions("<b>AccNo:</b> 0001872100-26-000003") == {"0001872100-26-000003"}
    # Prose carries none, so the rule cannot fire on ordinary coverage.
    assert accessions("NASA calls off Swift rescue mission") == frozenset()
    assert identifiers("no long digit runs here") == frozenset()


def test_the_identity_veto_leaves_prose_alone():
    """It fires only when both sides carry identifiers, so ordinary coverage — which carries
    none — is untouched, and one-sided evidence is not read as disagreement."""
    ap = ("NASA calls off Swift rescue mission", "")
    ars = (
        "NASA calls off mission to rescue Swift gamma-ray observatory",
        "<p>Filing 0001081400 was unrelated. " + "The observatory could not be reached. " * 20,
    )
    assert is_same_story(*ap, *ars)


def test_thresholds_stay_ordered():
    """A body is long enough that incidental overlap accumulates, so it must never be the
    looser of the two. Cheap guard against a retune inverting them by accident."""
    assert 0.0 < TITLE_JACCARD <= 1.0
    assert 0.0 < BODY_JACCARD <= 1.0


def test_jaccard_is_safe_on_empty_input():
    assert jaccard(frozenset(), content_tokens("anything")) == 0.0


def test_unrelated_stories_are_not_merged(polled):
    """Over-merging is the failure that makes a brief useless; assert it does not happen."""
    documents, _ = polled
    articles = _polled_articles(documents)
    deduped, _ = exact_dedup([a for a in articles if not a["parse_error"]])
    clusters = group_stories(deduped).clusters

    # story_key is the fixture's ground truth: no cluster may mix two of them.
    for cluster in clusters:
        members = [a for a in deduped if a["title"] == cluster["title"]]
        assert len({m["story_key"] for m in members}) == 1
    assert len(clusters) >= 5, "distinct events must stay distinct"
