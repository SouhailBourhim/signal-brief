"""Bronze -> silver normalization, as a pure function. SPEC §9.

Kept free of Spark on purpose. The transformation is the part that carries the domain
rules worth testing — timestamp distrust, canonical URLs, publisher extraction — and the
Spark job in `spark/jobs/normalize.py` is a thin `mapInPandas` around it. Pure logic can
be unit-tested in milliseconds without a JVM; the distribution layer needs an integration
test but almost no unit tests.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from signal_core.hashing import content_hash, simhash64
from signal_core.timeutil import ensure_utc, timestamps_disagree

# Tracking parameters carry no meaning and split otherwise-identical URLs.
_TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid", "ref")


def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    kept = [
        pair
        for pair in parts.query.split("&")
        if pair and not pair.split("=")[0].lower().startswith(_TRACKING_PREFIXES)
    ]
    return urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), "&".join(kept), "")
    )


def publisher_domain(url: str) -> str:
    return urlsplit(url).netloc.lower().removeprefix("www.")


def _parse_published(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value)))
    except (TypeError, ValueError):
        # A malformed timestamp is data about the source, not a reason to drop the
        # article. It becomes a disagreement flag rather than an exception.
        return None


def normalize_document(row: dict[str, Any]) -> dict[str, Any]:
    """One bronze row -> one `silver.articles` row.

    Raises nothing on bad input by design: SPEC §6.2 says failed records are quarantined
    with a reason, never silently dropped, so the caller inspects `parse_error` rather
    than catching exceptions.
    """
    fetched_at = ensure_utc(row["fetched_at"])
    base = {
        "article_id": "",
        "source_id": row["source_id"],
        "url_canonical": "",
        "title": "",
        "body_text": "",
        "published_at": None,
        "fetched_at": fetched_at,
        "lang": "en",
        "publisher_domain": "",
        "authority_score": 0.5,
        "simhash": 0,
        "content_hash": row["content_hash"],
        "timestamp_flagged": True,
        "story_key": None,
        "parse_error": None,
    }

    try:
        payload = json.loads(row["payload"])
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        return {**base, "parse_error": f"payload_not_json: {exc}"}

    title = (payload.get("title") or "").strip()
    body = (payload.get("body") or "").strip()
    url = payload.get("url") or ""
    if not title or not url:
        return {**base, "parse_error": "missing_title_or_url"}

    published_at = _parse_published(payload.get("published_at"))
    url_canonical = canonical_url(url)

    return {
        **base,
        # Identity is content + canonical URL, so the same article re-fetched under a
        # tracking-parameter variant is one article, not two.
        "article_id": content_hash(f"{url_canonical}\x1f{title}"),
        "url_canonical": url_canonical,
        "title": title,
        "body_text": body,
        "published_at": published_at,
        "publisher_domain": publisher_domain(url),
        "simhash": simhash64(f"{title} {body}"),
        "content_hash": content_hash(f"{title} {body}"),
        "timestamp_flagged": timestamps_disagree(fetched_at, published_at),
        "story_key": payload.get("story_key"),
    }
