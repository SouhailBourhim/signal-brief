"""Parser tests. docs/runbooks/phase-2.md 2.B.

Every fixture in `tests/fixtures/bronze/` is real bytes captured from a live poll on
2026-08-19 (`.cache/staging`, synced from `s3://signal-bronze-481879233905/staging`),
except `hackernews/job.json`: no real `type=="job"` item exists in the captured window,
so that one fixture is hand-built to HN's documented API shape — see
`signal_core/parse/hackernews.py`'s docstring. A test built on captured bytes fails when
a publisher changes; one built on imagined shape fails when someone's imagination does.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from signal_core.parse import REGISTRY, get_parser
from signal_core.parse.edgar import parse as parse_edgar
from signal_core.parse.fake import parse as parse_fake
from signal_core.parse.feedparse import (
    child_text,
    local_name,
    parse_rfc822,
    parse_rfc3339,
    parse_xml,
)
from signal_core.parse.hackernews import parse as parse_hackernews
from signal_core.parse.hn_scores import parse as parse_hn_scores
from signal_core.parse.market import parse as parse_market
from signal_core.parse.rss import parse_rss_ars, parse_rss_tech, parse_rss_verge

FIXTURES = Path(__file__).parent / "fixtures" / "bronze"


def _fixture(source: str, name: str) -> bytes:
    return (FIXTURES / source / name).read_bytes()


# --- registry -----------------------------------------------------------------------


def test_registry_covers_every_deployed_source():
    """Mirrors `test_source_registry.py`'s parity check on the sources side."""
    from signal_core.config import SOURCES

    assert set(REGISTRY) == set(SOURCES)


def test_get_parser_unknown_source_raises():
    with pytest.raises(KeyError, match="unknown source"):
        get_parser("nope")


def test_edgar_and_edgar_formd_share_one_parser():
    """`type=D` only narrows the feed; the entry shape is identical."""
    assert REGISTRY["edgar"] is REGISTRY["edgar_formd"]


# --- feedparse.py: the shared RFC 822 / RFC 3339 fix ---------------------------------


def test_parse_rfc822_reads_rss_pubdate():
    """The date bug: `datetime.fromisoformat` cannot read this format at all."""
    dt = parse_rfc822("Tue, 18 Aug 2026 22:14:25 +0000")
    assert dt == datetime(2026, 8, 18, 22, 14, 25, tzinfo=UTC)


def test_parse_rfc822_rejects_garbage_without_raising():
    assert parse_rfc822("not a date") is None
    assert parse_rfc822("") is None


def test_parse_rfc3339_reads_atom_published():
    dt = parse_rfc3339("2026-08-18T16:01:01-04:00")
    assert dt == datetime(2026, 8, 18, 20, 1, 1, tzinfo=UTC)


def test_parse_xml_falls_back_on_garbage_and_flags_it():
    """The 403 page real bronze captured for `edgar`: not well-formed XML, and not the
    feed root either. `parse_feed` must not raise."""
    result = parse_rss_tech(_fixture("edgar", "error.html"))
    assert result.error is not None
    assert not result.items


# --- rss_tech: RSS 2.0, RFC 822 dates -------------------------------------------------


def test_rss_tech_parses_the_first_real_item():
    result = parse_rss_tech(_fixture("rss_tech", "feed.xml"))
    assert result.error is None
    assert len(result.items) > 1, "a real feed page is many items, not one"

    first = result.items[0]
    assert first.title == (
        "Cursor capitalizes on GitHub frustration, launches rival hosting platform"
    )
    assert first.url == (
        "https://techcrunch.com/2026/08/18/cursor-capitalizes-on-github-frustration-"
        "launches-rival-hosting-platform/"
    )
    # guid (isPermaLink="false") is a distinct id from the article link — must not be
    # silently collapsed into the URL.
    assert first.external_id == "https://techcrunch.com/?p=3154081"
    assert first.external_id != first.url
    assert first.published_at == datetime(2026, 8, 18, 22, 14, 25, tzinfo=UTC)
    assert "Cursor" in first.body
    assert first.parse_error is None


def test_rss_tech_every_item_parses_without_error():
    """No parse_error on any real captured item — the fixture is real production RSS,
    so a failure here means the walker's assumptions are wrong, not the data."""
    result = parse_rss_tech(_fixture("rss_tech", "feed.xml"))
    assert all(item.parse_error is None for item in result.items)
    assert all(item.published_at is not None for item in result.items), (
        "every TechCrunch item carries pubDate; a None here would mean the RFC 822 fix regressed"
    )


# --- rss_ars: RSS 2.0, content:encoded, source #6 -------------------------------------


def test_rss_ars_parses_the_first_real_item():
    result = parse_rss_ars(_fixture("rss_ars", "feed.xml"))
    assert result.error is None
    first = result.items[0]
    assert (
        first.title
        == '"Sabotage": Experts, lawmakers blast RFK Jr. for destroying healthcare research'
    )
    assert first.url.startswith("https://arstechnica.com/health/2026/08/sabotage")
    # isPermaLink="true": guid and link happen to be the same URL here.
    assert first.external_id == first.url
    assert first.published_at == datetime(2026, 8, 18, 22, 32, 46, tzinfo=UTC)
    assert first.body, "content:encoded/description must not be dropped"


# --- rss_verge: Atom, RFC 3339 dates ---------------------------------------------------


def test_rss_verge_parses_the_first_real_entry():
    result = parse_rss_verge(_fixture("rss_verge", "feed.xml"))
    assert result.error is None
    first = result.items[0]
    assert "SteelSeries" in first.title
    assert (
        first.url == "https://www.theverge.com/gadgets/981611/steelseries-arctis-nova-3p-deal-sale"
    )
    assert first.external_id == "https://www.theverge.com/?p=981611"
    # `published`, not `updated` — they differ in the real fixture (16:01 vs 16:03).
    assert first.published_at == datetime(2026, 8, 18, 20, 1, 1, tzinfo=UTC)
    assert first.parse_error is None


# --- edgar / edgar_formd: Atom, ISO-8859-1, category-based form type ------------------


def test_edgar_form_type_and_cik_come_off_the_real_entry():
    result = parse_edgar(_fixture("edgar", "feed.xml"))
    assert result.error is None
    first = result.items[0]
    assert first.title == "4 - Koss Jennifer G. (0001872100) (Reporting)"
    assert first.extra["form_type"] == "4"  # from <category term="4">, not the title
    assert first.extra["cik"] == "0001872100"
    assert first.extra["filer_name"] == "Koss Jennifer G."
    assert first.url.endswith("0001872100-26-000003-index.htm")
    assert first.external_id == "urn:tag:sec.gov,2008:accession-number=0001872100-26-000003"
    # No <published> in EDGAR's Atom — falls back to <updated>.
    assert first.published_at == datetime(2026, 8, 19, 1, 59, 50, tzinfo=UTC)


def test_edgar_formd_form_type_is_d():
    result = parse_edgar(_fixture("edgar_formd", "feed.xml"))
    first = result.items[0]
    assert first.extra["form_type"] == "D"
    assert first.extra["cik"] == "0002144150"
    assert first.extra["filer_name"] == "Klondike Holdings LLC"


def test_edgar_iso_8859_1_declaration_parses_from_bytes():
    """The concrete case `staging.to_record`'s base64 exists for. If this were decoded
    as UTF-8 before parsing, it would either raise or mojibake; ElementTree.fromstring
    on raw bytes lets expat honor the declared encoding."""
    payload = _fixture("edgar", "feed.xml")
    assert b'encoding="ISO-8859-1"' in payload[:60]
    result = parse_edgar(payload)
    assert result.error is None
    assert len(result.items) > 1


def test_edgar_error_page_is_a_row_level_error_not_a_crash():
    """Real captured `outcome=error` bronze: SEC's 403 page. The caller is expected to
    skip `error`/`empty` rows before calling a parser at all (2.B decision), but the
    parser itself must degrade to a row-level error rather than raise if it's ever
    handed one anyway."""
    result = parse_edgar(_fixture("edgar", "error.html"))
    assert result.error is not None
    assert not result.items


# --- hackernews: type routing, EMPTY skip, dead items ---------------------------------


def test_hackernews_story_becomes_an_item():
    result = parse_hackernews(_fixture("hackernews", "story.json"))
    assert not result.comments
    (item,) = result.items
    assert item.title == "AI Is Upending One of Finance's Cushiest Jobs"
    assert item.url.startswith("https://www.bloomberg.com/")
    assert item.external_id == "49350858"
    assert item.published_at == datetime.fromtimestamp(1787079578, tz=UTC)
    assert item.extra["type"] == "story"
    assert item.parse_error is None


def test_hackernews_job_becomes_an_item():
    """Hand-built fixture — see module docstring. Confirms `type in (story, job)` is
    genuinely both, not just `story`."""
    result = parse_hackernews(_fixture("hackernews", "job.json"))
    (item,) = result.items
    assert item.extra["type"] == "job"
    assert item.title == "Signal Brief Labs Is Hiring a Data Engineer"


def test_hackernews_comment_becomes_a_comment_not_an_article():
    """The velocity finding's whole reason for `silver.hn_comments` existing."""
    result = parse_hackernews(_fixture("hackernews", "comment.json"))
    assert not result.items
    (comment,) = result.comments
    assert comment.item_id == "49350860"
    assert comment.parent_id == "49348545"
    assert comment.by == "ericmay"
    assert comment.text


def test_hackernews_ask_story_has_no_url_falls_back_to_discussion_link():
    result = parse_hackernews(_fixture("hackernews", "ask_story.json"))
    (item,) = result.items
    assert item.url == "https://news.ycombinator.com/item?id=49351430"
    assert item.parse_error is None


def test_hackernews_dead_story_has_no_title_and_is_quarantined_not_dropped():
    """SPEC §6.2: a dead item with nothing to show is still a record, not a hole."""
    result = parse_hackernews(_fixture("hackernews", "dead_story.json"))
    (item,) = result.items
    assert item.parse_error == "missing_title"
    assert item.extra == {}  # quarantined branch never reaches the extra-building code


def test_hackernews_empty_outcome_payload_parses_to_nothing():
    """A bare `null` body — HN's shape for a deleted/not-yet-visible id."""
    result = parse_hackernews(_fixture("hackernews", "empty.json"))
    assert result == type(result)()  # items=[], comments=[], error=None, warnings=()


def test_hackernews_unhandled_type_is_flagged_not_silently_absorbed():
    result = parse_hackernews(b'{"id": 1, "type": "pollopt", "time": 1787079578}')
    assert not result.items and not result.comments
    assert result.error is None
    assert "unhandled_hn_type" in result.warnings[0]


# --- hn_scores: the same bytes, a different record ------------------------------------


def test_hn_scores_reads_the_same_payload_as_hackernews_and_keeps_the_score():
    """Deliberately the *same* fixture the article parser uses. The two sources hit one
    endpoint and the payloads are identical; what differs is which field is the record.
    Reusing the fixture is what keeps that claim honest — if the shapes ever diverge, this
    fails rather than a hand-written fixture agreeing with a stale assumption."""
    result = parse_hn_scores(_fixture("hackernews", "story.json"))
    assert not result.items, "a score snapshot is not an article"
    assert not result.comments
    (snapshot,) = result.score_snapshots
    assert snapshot.item_id == "49350858"
    assert snapshot.score == 1
    assert snapshot.descendants == 0
    assert snapshot.title == "AI Is Upending One of Finance's Cushiest Jobs"


def test_hn_scores_ignores_comments_rather_than_scoring_them():
    result = parse_hn_scores(_fixture("hackernews", "comment.json"))
    assert not result.score_snapshots
    assert result.warnings and "unhandled_hn_type" in result.warnings[0]


def test_hn_scores_empty_payload_parses_to_nothing():
    assert parse_hn_scores(_fixture("hackernews", "empty.json")).score_snapshots == []


def test_a_story_with_no_score_contributes_no_snapshot():
    """Not a zero. A zero is a real observation and would drag a slope down; a story whose
    shape this parser does not understand should contribute nothing and say so."""
    result = parse_hn_scores(b'{"id": 1, "type": "story", "title": "t"}')
    assert result.score_snapshots == []
    assert result.error is None, "the row's bytes are fine; only this entry is unreadable"
    assert result.warnings and "missing_score" in result.warnings[0]


def test_hn_scores_rejects_a_non_json_payload():
    assert parse_hn_scores(b"<html>nope").error is not None


# --- market: Yahoo chart JSON, real captured bytes ------------------------------------


def test_market_parses_a_real_chart_response():
    """`chart_aapl.json` is a live capture (2026-08-22), not a hand-built shape — the same
    rule every other fixture here follows."""
    result = parse_market(_fixture("market", "chart_aapl.json"))
    assert result.error is None
    assert not result.items, "a price bar is not an article"
    assert len(result.market_observations) == 63, "range=3mo should carry its own baseline"

    latest = result.market_observations[-1]
    assert latest.ticker == "AAPL"
    assert latest.close > 0
    assert latest.high >= latest.low
    # Ordered oldest-first, which is what a trailing-window calculation expects.
    dates = [o.trade_date for o in result.market_observations]
    assert dates == sorted(dates)


def test_market_reports_an_unknown_ticker_as_a_row_level_error():
    """Also a live capture. Yahoo answers a delisted or misspelled symbol in-band, so this
    has to land in `parse_rejects` rather than looking like a ticker with no trading days."""
    result = parse_market(_fixture("market", "chart_unknown.json"))
    assert result.error is not None
    assert "chart_error" in result.error
    assert result.market_observations == []


def test_a_bar_with_no_close_is_dropped_and_counted():
    """A halted session. Zero-filling would be a catastrophic price move to anything
    computing returns; the bronze row still holds the original bytes (SPEC §6.2)."""
    payload = json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {"symbol": "AAPL"},
                        "timestamp": [1786060800, 1786147200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [1.0, None],
                                    "high": [2.0, None],
                                    "low": [0.5, None],
                                    "close": [1.5, None],
                                    "volume": [100, None],
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }
    ).encode()

    result = parse_market(payload)
    assert len(result.market_observations) == 1
    assert result.warnings and "dropped_bars_without_close" in result.warnings[0]


def test_a_bar_with_a_close_but_a_null_open_keeps_the_bar():
    """The close is the only field the ranker reads, so a partial bar is still usable."""
    payload = json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {"symbol": "NVDA"},
                        "timestamp": [1786060800],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [None],
                                    "high": [None],
                                    "low": [None],
                                    "close": [7.5],
                                    "volume": [None],
                                }
                            ]
                        },
                    }
                ]
            }
        }
    ).encode()

    (observation,) = parse_market(payload).market_observations
    assert observation.close == 7.5
    assert observation.open == 7.5, "falls back to the close rather than dropping the bar"
    assert observation.volume is None


def test_market_rejects_a_non_json_payload():
    assert parse_market(b"<html>nope").error is not None


# --- fake: Phase 0 shape, now just another registry entry -----------------------------


def test_fake_parses_a_real_captured_article():
    result = parse_fake(_fixture("fake", "article.json"))
    (item,) = result.items
    assert item.title == "Northwind acquires Lumen Robotics for $2.4B"
    assert item.extra["story_key"] == "acq"
    assert item.extra["publisher"] == "techcrunch.com"
    assert item.published_at is not None
    assert item.parse_error is None


def test_fake_missing_published_at_is_none_not_a_crash():
    """SPEC §6.2's honest-distrust case: the formd fixture item has no published_at."""
    result = parse_fake(_fixture("fake", "missing_published_at.json"))
    (item,) = result.items
    assert item.published_at is None
    assert item.parse_error is None


def test_fake_malformed_payload_is_a_row_level_error():
    result = parse_fake(b"not json at all")
    assert result.error is not None and result.error.startswith("payload_not_json")


# --- shared XML primitives, exercised directly -----------------------------------------


def test_local_name_strips_namespace():
    assert local_name("{http://www.w3.org/2005/Atom}entry") == "entry"
    assert local_name("item") == "item"


def test_child_text_ignores_grandchildren():
    root, _ = parse_xml(b"<a><b>x<c>y</c></b><b>z</b></a>")
    # only the *first* direct <b> child's own text, not its descendant's
    assert child_text(root, "b") == "x"
