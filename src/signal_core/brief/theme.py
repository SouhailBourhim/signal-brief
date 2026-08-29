"""The brief's palette and label vocabulary. SPEC §11, §16.5.

## Why this is a module and not CSS

The brief is read in two places with very different capabilities. In a browser it is an
ordinary page. In Gmail — which ADR-0013 makes the delivery path that actually matters — it
is passed through a sanitiser that **drops CSS custom properties**, so a stylesheet built on
`var(--fg)` arrives with every colour unresolved and renders as unstyled text on white. That
was the real reason the mailed brief looked plain: not the design, but that the design never
reached the reader.

The fix is to emit literal hex into `style=` attributes, which survives sanitising. Doing
that from a stylesheet's worth of colours would mean scattering the same strings through the
template and letting them drift. So the palette lives here, in one place, and the template
interpolates named values. Same reasoning as `FEED_BOILERPLATE` being shared between
`dedup.py` and `entities/resolve.py`: a second copy eventually disagrees with the first.

## Why every topic gets a colour, and why the map is exhaustive

`enrich.schema.Topic` is a closed enum, and its docstring argues for closure on the grounds
that an open field makes classification a measurement of vocabulary drift. The same argument
applies downstream: a closed enum with an incomplete style map produces a page where some
stories are quietly colourless, which reads as a design flaw rather than as missing data.
`tests/test_brief_theme.py` asserts every member is covered.

`topic` is nullable — a cluster the enrichment stage never reached has `None` — so `FALLBACK`
is a real, styled entry rather than a hole. A story that was never enriched should look
unenriched, not broken.

## Contrast

Every `fg` on its own `tint` clears WCAG AA (4.5:1) in both schemes; the lowest pair is
`business-corporate` at 5.2 light and 6.2 dark. Checked when the palette was written, because
a topic pill is small text on a tinted ground, which is exactly where a hand-picked palette
usually fails.
"""

from __future__ import annotations

from dataclasses import dataclass

from signal_core.enrich.schema import Topic


@dataclass(frozen=True)
class TopicStyle:
    """How one topic presents: its human label and its colour in both schemes."""

    label: str
    fg: str
    tint: str
    fg_dark: str
    tint_dark: str


# Keyed by `Topic` value rather than by the enum member, because the value is what survives
# the round trip through `gold.cluster_enrichment` and reaches the template as a plain string.
TOPIC_STYLES: dict[str, TopicStyle] = {
    Topic.AI_ML: TopicStyle("AI & ML", "#5b3fd6", "#eeeaff", "#b3a4ff", "#262046"),
    Topic.SECURITY: TopicStyle("Security", "#b3261e", "#fdeceb", "#f0938c", "#3a1e1c"),
    Topic.SOFTWARE_ENGINEERING: TopicStyle(
        "Software Engineering", "#1f5fa8", "#e7f0fa", "#8fbdf0", "#16263a"
    ),
    Topic.HARDWARE_DEVICES: TopicStyle("Hardware", "#0e6b62", "#e0f3f1", "#6fc9be", "#10302d"),
    Topic.SCIENCE_RESEARCH: TopicStyle("Science", "#3742a8", "#eaecfa", "#9fa8ef", "#1e2140"),
    Topic.BUSINESS_CORPORATE: TopicStyle("Business", "#8a5a2b", "#f8f0e6", "#d0a068", "#33261a"),
    Topic.POLICY_REGULATION: TopicStyle("Policy", "#475569", "#eef1f5", "#a8b6c8", "#22272e"),
    Topic.SEC_FILING: TopicStyle("SEC Filing", "#55632f", "#f0f4e3", "#b3c47a", "#262a18"),
    Topic.SOCIETY_CULTURE: TopicStyle("Society", "#9c2f70", "#fbeaf3", "#e894c2", "#37182a"),
    Topic.OTHER: TopicStyle("Other", "#64645d", "#f1f0ec", "#a8a79f", "#26262a"),
}

# Used for `topic is None` (enrichment never ran for this cluster) and for any value that is
# not in the enum — which should be impossible, but a brief that renders is worth more than a
# brief that raises on a value the store disagrees with.
FALLBACK = TopicStyle("Unclassified", "#6f6d66", "#f1f0ec", "#9a978f", "#26262a")


def topic_style(topic: str | None) -> TopicStyle:
    """The style for `topic`, falling back rather than raising. See `FALLBACK`."""
    if topic is None:
        return FALLBACK
    return TOPIC_STYLES.get(topic, FALLBACK)


# `market_corroboration` is 20 characters and shares a line with five other names. The scores
# are labelled for a reader skimming why a story ranked, not for a reader reconstructing
# `ranker.WEIGHTS` — the full-fidelity record of that is `gold.brief_items.score_components`,
# which keeps the unabbreviated keys.
COMPONENT_LABELS: dict[str, str] = {
    "breadth": "breadth",
    "relevance": "relevance",
    "recency": "recency",
    "velocity": "velocity",
    "market_corroboration": "market",
    "feedback": "feedback",
}


def component_label(key: str) -> str:
    return COMPONENT_LABELS.get(key, key)


# The page's neutrals and status colours, in one place for the same reason as the topics.
# `thin` and the stale banner deliberately share `#9a7b1f`: "something is off" should read
# identically whether it is one source under its floor or the whole brief being a day late.
PALETTE: dict[str, str] = {
    "page": "#f4f2ee",
    "card": "#ffffff",
    "rule": "#e4e1da",
    "head": "#17171a",
    "body": "#3d3b36",
    "muted": "#6f6d66",
    "ok": "#3f7d3f",
    "thin": "#9a7b1f",
    "degraded": "#a34141",
    "ok_tint": "#e8f2e8",
    "thin_tint": "#fdf8e8",
    "degraded_tint": "#fbeceb",
    "stale_bg": "#fdf8e8",
    "stale_fg": "#5a4a12",
    "page_dark": "#131316",
    "card_dark": "#1c1c20",
    "rule_dark": "#2c2c33",
    "head_dark": "#eceae5",
    "body_dark": "#cfccc4",
    "muted_dark": "#9a978f",
    "ok_dark": "#7ab77a",
    "thin_dark": "#d2b45c",
    "degraded_dark": "#e08585",
    "ok_tint_dark": "#18271a",
    "thin_tint_dark": "#241f10",
    "degraded_tint_dark": "#2e1a1a",
    "stale_bg_dark": "#241f10",
    "stale_fg_dark": "#e4d7a8",
}
