"""The brief's palette, and the guards that keep the page reaching the reader styled.

`tests/test_lambda_artifact.py` fails the build if the poller's import chain grows a heavy
dependency. This file is the same idea aimed at the other end of the pipeline: the brief is
mailed byte-identical to the file it renders, and Gmail's sanitiser silently drops several
constructs a browser handles fine. A page that regresses to those looks correct in every
local check and arrives unstyled, which is how it went unnoticed for weeks.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest

from signal_core.brief.render import duration, facts, render_brief
from signal_core.brief.theme import FALLBACK, PALETTE, TOPIC_STYLES, topic_style
from signal_core.enrich.schema import Topic
from signal_core.ops.health import RunHealth

NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)


def _cluster(**overrides):
    base = {
        "title": "Northwind acquires Lumen",
        "url_canonical": "https://example.com/a",
        "publisher_domain": "example.com",
        "score": 0.42,
        "score_components": {},
        "rank": 1,
        "distinct_publisher_count": 1,
        "publishers": ["example.com"],
        "body_text": "Northwind acquired Lumen.",
        "entities": [],
        "summary": "Northwind acquired Lumen for cash.",
        "topic": "ai-ml",
        "last_seen": datetime(2026, 8, 29, 9, 0, tzinfo=UTC),
    }
    return base | overrides


def _render(clusters, **kwargs):
    return render_brief(
        clusters, RunHealth(articles_in=1, clusters_out=1), date="2026-08-29", now=NOW, **kwargs
    )


# --- the palette is complete ------------------------------------------------------------


@pytest.mark.parametrize("topic", list(Topic))
def test_every_topic_has_a_style(topic: Topic):
    """`Topic` is a closed enum; an incomplete style map makes some stories colourless.

    `enrich/schema.py` argues for closure so the eval scores classification rather than
    vocabulary drift. The same argument runs downstream: adding a topic without a colour
    would ship a page where a subset of cards are quietly unstyled.
    """
    assert str(topic) in TOPIC_STYLES
    style = TOPIC_STYLES[str(topic)]
    assert style.label and not style.label.islower()  # a human label, not the slug


def test_an_unenriched_story_falls_back_rather_than_raising():
    """`topic` is null whenever the enrichment stage never reached the cluster."""
    assert topic_style(None) is FALLBACK
    assert topic_style("a-topic-the-enum-does-not-have") is FALLBACK


def test_contrast_clears_wcag_aa_in_both_schemes():
    """A topic pill is small text on a tinted ground — where a hand-picked palette fails."""

    def relative_luminance(value: str) -> float:
        raw = [int(value.lstrip("#")[i : i + 2], 16) / 255 for i in (0, 2, 4)]
        linear = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in raw]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    def ratio(a: str, b: str) -> float:
        la, lb = relative_luminance(a), relative_luminance(b)
        return (max(la, lb) + 0.05) / (min(la, lb) + 0.05)

    for name, style in [*TOPIC_STYLES.items(), ("fallback", FALLBACK)]:
        assert ratio(style.fg, style.tint) >= 4.5, f"{name} light"
        assert ratio(style.fg_dark, style.tint_dark) >= 4.5, f"{name} dark"


# --- the page survives Gmail ------------------------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "why"),
    [
        (r"var\(--", "Gmail drops CSS custom properties; every colour would resolve to nothing"),
        (r"display:\s*(flex|grid)", "Gmail and Outlook implement neither flexbox nor grid"),
        (r"[\d.]\s*rem\b", "Outlook's Word engine mishandles rem units"),
    ],
)
def test_the_rendered_page_avoids_constructs_gmail_strips(pattern: str, why: str):
    """This is the regression that actually shipped, so it is the one with a test.

    The brief was built on `:root` custom properties and mailed unmodified. Gmail dropped the
    declarations, `var(--fg)` resolved to nothing, and five weeks of briefs arrived as black
    serif on white while every local check stayed green — the page cannot be checked by
    rendering it locally, only by mailing it.
    """
    html = _render([_cluster()])
    assert re.search(pattern, html) is None, why


def test_colours_are_inline_not_only_in_the_style_block():
    """Inline `style=` is what survives sanitising; the `<style>` block is enhancement."""
    html = _render([_cluster()])
    body = html.split("</head>", 1)[1]
    assert body.count('style="') > 20
    assert PALETTE["card"] in body and PALETTE["muted"] in body


def test_a_topic_colours_its_card():
    html = _render([_cluster(topic="security")])
    assert TOPIC_STYLES["security"].fg in html
    assert "Security" in html
    assert 'class="t-security"' in html


# --- score components -------------------------------------------------------------------


def test_a_component_that_did_not_fire_is_left_off_the_page():
    """SPEC §7.4 wants ranking explainable, not six numbers of which five read `0.00`.

    docs/runbooks/phase-5.md §5.C records that the shipped ranker is effectively
    single-component on the real corpus. The unabridged set stays in
    `gold.brief_items.score_components`.
    """
    html = _render([_cluster(score_components={"recency": 0.98, "breadth": 0.0, "velocity": 0.0})])
    assert "recency" in html
    assert "breadth" not in html
    assert "velocity" not in html


def test_a_downvote_still_shows_because_it_moved_the_score():
    """`feedback` is the one component that can subtract, so it is filtered on truthiness."""
    html = _render([_cluster(score_components={"feedback": -1.0, "breadth": 0.0})])
    assert "feedback" in html
    assert "-1.00" in html
    assert PALETTE["degraded"] in html


# --- helpers ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (None, "never"),
        (0, "0s"),
        (89, "89s"),
        (600, "10m"),
        (3600, "1h"),
        (7200, "2h"),
        (129600, "36h"),
        (172800, "2d"),
    ],
)
def test_duration_reads_as_the_coarsest_useful_unit(seconds, expected):
    assert duration(seconds) == expected


def test_a_stories_age_is_measured_from_the_same_instant_as_its_recency_score():
    html = _render([_cluster()])  # last_seen is three hours before NOW
    assert "3h ago" in html


def test_extraction_renders_only_the_fields_the_model_did_not_abstain_on():
    """`enrich/schema.py`: null is the expected answer most of the time."""
    assert facts(None) == []
    assert facts({"company": None, "amount_usd": None}) == []
    assert facts({"company": "Northwind", "amount_usd": 4.0e7, "round_type": "Series B"}) == [
        "Northwind",
        "$40M",
        "Series B",
    ]
    assert facts({"headcount_delta": -300, "filing_type": "8-K"}) == ["-300 jobs", "Form 8-K"]


def test_extraction_reaches_the_page():
    html = _render([_cluster(extraction={"company": "Northwind", "amount_usd": 2.4e9})])
    assert "$2.4B" in html


# --- the health footer ------------------------------------------------------------------


def test_a_source_with_no_gap_gets_no_gap_row():
    """The previous layout emitted an empty `<td class="degraded">` for every healthy source."""
    health = RunHealth(articles_in=1, clusters_out=1)
    healthy = render_brief([_cluster()], health, date="2026-08-29", now=NOW)
    assert 'colspan="4"' not in healthy
