"""HTML brief rendering. SPEC §11 (health footer), §16 (screenshot-worthy output)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from signal_core.ops.health import RunHealth
from signal_core.timeutil import brief_date

_TEMPLATES = Path(__file__).parent / "templates"


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
    return template.render(
        date=date or brief_date(),
        clusters=[c for c in clusters if c.get("included", True)],
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
