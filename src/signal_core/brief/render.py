"""HTML brief rendering. SPEC §11 (health footer), §16 (screenshot-worthy output)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from signal_core.brief.theme import (
    FALLBACK,
    PALETTE,
    TOPIC_STYLES,
    component_label,
)
from signal_core.dedup import strip_boilerplate
from signal_core.ops.health import RunHealth
from signal_core.timeutil import brief_date, ensure_utc, utc_now

_TEMPLATES = Path(__file__).parent / "templates"


# How much prose goes under a headline. Long enough to say what happened, short enough that
# ten of them are still a brief rather than a reader.
SNIPPET_CHARS = 400


def snippet(body_text: str) -> str:
    """`body_text` as prose a person can read, not as the markup the feed sent.

    **Found by reading the brief, which is the only way it could have been.** Every test
    passed and both eval gates were green while the page showed, under a Tesla headline,
    `<figure><img alt="Tesla Solar Roof Event photos" data-portal-copyright="Image: Dieter
    Bohn / <em>The Verge</em>" ...>`. The template autoescapes — correctly, since this is
    untrusted feed content — so raw markup renders as visible tags rather than as formatting,
    and half of each snippet was image attributes.

    `dedup.strip_boilerplate` already does exactly this cleaning for SPEC §7.1 stage 1, and
    it is reused rather than reimplemented for the same reason `FEED_BOILERPLATE` is shared
    with `entities/resolve.py`: a second copy would eventually disagree with the first about
    what the feeds emit, and then the brief would show something the clusterer never saw.

    Truncation is at a word boundary, and the ellipsis is only added when something was
    actually cut — a snippet that fits should not claim there is more.
    """
    text = " ".join(strip_boilerplate(body_text).split())
    if len(text) <= SNIPPET_CHARS:
        return text
    cut = text[:SNIPPET_CHARS]
    boundary = cut.rfind(" ")
    return (cut[:boundary] if boundary > 0 else cut).rstrip(",;:.") + " …"


def duration(seconds: float | int | None) -> str:
    """`seconds` as the coarsest unit that still says something. `None` reads as "never".

    The health footer used to print raw seconds — "97s ago", "86400s ago" — which is precise
    and unreadable at the same time, and gets worse the more wrong things are. A number a
    reader has to divide is a number they skip.
    """
    if seconds is None:
        return "never"
    seconds = max(float(seconds), 0.0)
    # Thresholds are set on the *rounded* value, not the raw one, so an hour reads "1h"
    # rather than "60m" and two days read "2d" rather than "48h".
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds / 60 < 59.5:
        return f"{seconds / 60:.0f}m"
    if seconds / 3600 < 47.5:
        return f"{seconds / 3600:.0f}h"
    return f"{seconds / 86400:.0f}d"


def _money(amount: float) -> str:
    """A dollar figure at the magnitude the reader cares about, not to the cent."""
    for cutoff, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(amount) >= cutoff:
            scaled = amount / cutoff
            digits = 0 if abs(scaled) >= 100 else 1
            return f"${scaled:.{digits}f}{suffix}".replace(".0", "")
    return f"${amount:,.0f}"


def facts(extraction: dict[str, Any] | None) -> list[str]:
    """SPEC §7.3's extraction fields, as chips, skipping the ones the model abstained on.

    `enrich/schema.py` is explicit that "every field is nullable and null is the expected
    answer most of the time" — a story about a Go release has no round type. Rendering a
    label for a null would present an abstention as a missing value, which is the opposite of
    what the schema's nullability was for. So only non-null fields become chips, and a story
    with nothing extracted shows no row at all.
    """
    if not extraction:
        return []
    out: list[str] = []
    company = extraction.get("company")
    if company:
        out.append(str(company))
    amount = extraction.get("amount_usd")
    if amount is not None:
        out.append(_money(float(amount)))
    round_type = extraction.get("round_type")
    if round_type:
        out.append(str(round_type))
    delta = extraction.get("headcount_delta")
    if delta:
        out.append(f"{int(delta):+d} jobs")
    filing = extraction.get("filing_type")
    if filing:
        out.append(f"Form {filing}")
    return out


def age(cluster: dict[str, Any], now: datetime) -> str | None:
    """How long ago the story was last covered, or None if the cluster carries no timestamp.

    Reads `last_seen` falling back to `fetched_at`, the same reference `ranker.score_cluster`
    measures recency from — so the "3h ago" on the card and the `recency` component beside it
    cannot describe different instants.
    """
    reference = cluster.get("last_seen") or cluster.get("fetched_at")
    if reference is None:
        return None
    elapsed = (now - ensure_utc(reference)).total_seconds()
    return f"{duration(elapsed)} ago"


def _environment() -> Environment:
    env = Environment(
        loader=FileSystemLoader(_TEMPLATES),
        # autoescape=True, not select_autoescape(["html"]): that helper keys off the
        # filename suffix, and these templates end in `.j2`, which silently disabled
        # escaping. Titles come from third-party feeds and are never trusted markup.
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # Registered as filters rather than passed per-cluster: they are presentation vocabulary,
    # and threading them through the context would put a lookup table in every row.
    env.filters["component_label"] = component_label
    env.filters["duration"] = duration
    return env


# Keyed by the plain string that reaches the template, plus the fallback the card loop names
# when `topic` is null. Built once: `Topic` is a closed enum and this does not change at run
# time.
_TOPIC_STYLES = {str(key): value for key, value in TOPIC_STYLES.items()} | {
    "unclassified": FALLBACK
}


def render_brief(
    clusters: list[dict[str, Any]],
    health: RunHealth,
    date: str | None = None,
    revisions: list[Any] | None = None,
    *,
    stale_since: str | None = None,
    now: datetime | None = None,
    streak: Any | None = None,
) -> str:
    template = _environment().get_template("brief.html.j2")
    # Injectable for the same reason `ranker.score_cluster` takes one: the page states ages
    # relative to a moment, and a test that cannot fix that moment cannot assert on them.
    now = now or utc_now()
    # Cleaned here rather than by whoever assembled the clusters, because every path that
    # renders needs it and they do not share a builder: `brief/build.py` reads Athena,
    # `skeleton.py` runs `group_stories` in process. Doing it in the Athena reader alone left
    # the skeleton's brief with no body text at all — caught by reading that brief, which is
    # the same lesson this phase kept relearning.
    shown = [
        dict(
            c,
            snippet=snippet(c.get("body_text", "")),
            facts=facts(c.get("extraction")),
            age=age(c, now),
        )
        for c in clusters
        if c.get("included", True)
    ]
    return template.render(
        date=date or brief_date(),
        clusters=shown,
        health=health.to_dict(),
        # SPEC §8's payoff line. Defaults to empty so `skeleton.py` — which has no macro
        # store and never will — renders the same template without knowing about it.
        revisions=revisions or [],
        # When set, the newest clustered window predates this brief's own date — the stories
        # below are not today's. Defaults to None so `skeleton.py`, which clusters in process
        # and has no chain to fall behind, renders the same template without knowing about it.
        stale_since=stale_since,
        # SPEC §16.5's consecutive-day count. Its own variable rather than a `RunHealth`
        # field: `health` is what this run measured, the streak is what the series has done,
        # and folding one into the other would make a fact about history look like a fact
        # about today. Defaults to None so `skeleton.py` — which has no `gold.brief_items`
        # and never will — renders the same template without knowing about it.
        streak=streak,
        # The palette is data, not CSS: Gmail drops custom properties, so the colours have to
        # be interpolated into inline styles as literal hex. See `brief/theme.py`.
        palette=PALETTE,
        topic_styles=_TOPIC_STYLES,
    )


def write_brief(
    clusters: list[dict[str, Any]],
    health: RunHealth,
    out_root: Path,
    date: str | None = None,
    revisions: list[Any] | None = None,
    *,
    stale_since: str | None = None,
    now: datetime | None = None,
    streak: Any | None = None,
) -> Path:
    date = date or brief_date()
    out_root.mkdir(parents=True, exist_ok=True)
    path = out_root / f"brief-{date}.html"
    path.write_text(
        render_brief(
            clusters,
            health,
            date,
            revisions,
            stale_since=stale_since,
            now=now,
            streak=streak,
        ),
        encoding="utf-8",
    )
    return path
