"""Parsed item -> silver row, as a pure function. SPEC §9.

Kept free of Spark on purpose. The transformation is the part that carries the domain
rules worth testing — timestamp distrust, canonical URLs, publisher extraction — and the
Spark job in `spark/jobs/normalize.py` is a thin `mapInPandas` around it. Pure logic can
be unit-tested in milliseconds without a JVM; the distribution layer needs an integration
test but almost no unit tests.

Until docs/runbooks/phase-2.md's 2.B, this module also parsed bronze payload bytes
itself (`normalize_document`), hardcoded to the Phase 0 fake source's JSON shape — the
only shape that existed. 2.B split that: `signal_core.parse.get_parser(source_id)` turns
bronze bytes into zero or more `ParsedItem`s, source by source, and `to_article` below
is the source-agnostic second half, turning one of those plus its bronze row's fetch
context into the row `silver.articles` actually stores.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

from signal_core.hashing import content_hash, simhash64
from signal_core.parse.models import ParsedItem
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


def to_article(parsed: ParsedItem, bronze_row: dict[str, Any]) -> dict[str, Any]:
    """One `ParsedItem`, plus the bronze row it came from, -> one `silver.articles` row.

    Raises nothing on bad input by design: SPEC §6.2 says failed records are quarantined
    with a reason, never silently dropped, so the caller inspects `parse_error` rather
    than catching exceptions. A `ParsedItem` that already carries a `parse_error` (its
    parser found it missing a title or URL) is quarantined here too, unchanged.
    """
    fetched_at = ensure_utc(bronze_row["fetched_at"])
    base = {
        "article_id": "",
        "source_id": bronze_row["source_id"],
        "url_canonical": "",
        "title": parsed.title,
        "body_text": parsed.body,
        "published_at": None,
        "fetched_at": fetched_at,
        # ADR-0007: `published_at` is nullable by design (SPEC §6.2 trusts no
        # timestamp), and a null partition key can't be pruned — every source that
        # omits or mangles a date would otherwise land in one ever-growing partition
        # scanned by every date-bounded query. Falls back to `fetched_at`, which is
        # never null, so it's always the coalesced value even before `published_at`
        # is known below.
        "event_date": fetched_at,
        "lang": parsed.lang,
        "publisher_domain": "",
        "authority_score": 0.5,
        "simhash": 0,
        # Falls back to the bronze row's own hash (the whole document) until a
        # content_hash can be computed from the parsed title/body below.
        "content_hash": bronze_row.get("content_hash", ""),
        "timestamp_flagged": True,
        "story_key": parsed.extra.get("story_key"),
        "parse_error": parsed.parse_error,
        # The source's own id, carried through rather than dropped here. SPEC §7.4's
        # velocity component joins a cluster's Hacker News member back to the score
        # snapshots taken of it, and an id the source assigned is the only stable key for
        # that — `article_id` is derived from content, so it changes when a headline is
        # edited, which is exactly when a story is still developing.
        "external_id": parsed.external_id or None,
    }

    if parsed.parse_error:
        return base
    if not parsed.title or not parsed.url:
        return {**base, "parse_error": "missing_title_or_url"}

    published_at = ensure_utc(parsed.published_at) if parsed.published_at else None
    url_canonical = canonical_url(parsed.url)

    return {
        **base,
        # Identity is content + canonical URL, so the same article re-fetched under a
        # tracking-parameter variant is one article, not two.
        "article_id": content_hash(f"{url_canonical}\x1f{parsed.title}"),
        "url_canonical": url_canonical,
        "published_at": published_at,
        "event_date": published_at or fetched_at,
        "publisher_domain": publisher_domain(parsed.url),
        "simhash": simhash64(f"{parsed.title} {parsed.body}"),
        "content_hash": content_hash(f"{parsed.title} {parsed.body}"),
        "timestamp_flagged": timestamps_disagree(fetched_at, published_at),
    }
