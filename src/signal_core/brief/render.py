"""HTML brief rendering. SPEC §11 (health footer), §16 (screenshot-worthy output)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from signal_core.dedup import strip_boilerplate
from signal_core.ops.health import RunHealth
from signal_core.timeutil import brief_date

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


def _environment() -> Environment:
    return Environment(
        loader=FileSystemLoader(_TEMPLATES),
        # autoescape=True, not select_autoescape(["html"]): that helper keys off the
        # filename suffix, and these templates end in `.j2`, which silently disabled
        # escaping. Titles come from third-party feeds and are never trusted markup.
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_brief(clusters: list[dict[str, Any]], health: RunHealth, date: str | None = None) -> str:
    template = _environment().get_template("brief.html.j2")
    # Cleaned here rather than by whoever assembled the clusters, because every path that
    # renders needs it and they do not share a builder: `brief/build.py` reads Athena,
    # `skeleton.py` runs `group_stories` in process. Doing it in the Athena reader alone left
    # the skeleton's brief with no body text at all — caught by reading that brief, which is
    # the same lesson this phase kept relearning.
    shown = [
        dict(c, snippet=snippet(c.get("body_text", "")))
        for c in clusters
        if c.get("included", True)
    ]
    return template.render(
        date=date or brief_date(),
        clusters=shown,
        health=health.to_dict(),
    )


def write_brief(
    clusters: list[dict[str, Any]], health: RunHealth, out_root: Path, date: str | None = None
) -> Path:
    date = date or brief_date()
    out_root.mkdir(parents=True, exist_ok=True)
    path = out_root / f"brief-{date}.html"
    path.write_text(render_brief(clusters, health, date), encoding="utf-8")
    return path
