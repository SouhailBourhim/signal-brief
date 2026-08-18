"""A deterministic in-memory source. Phase 0 only — no network, no AWS.

It exists to exercise the contract and the awkward paths before any real feed is
involved. Deliberately emits:

  * syndication — one story arriving from several publishers with rewritten headlines,
    which is the case §7.1 exists to collapse
  * byte-identical reprints — the exact-duplicate stage
  * a missing `published_at` and a future one — the §6.2 disagreement flag
  * a repeat of an already-seen id — the `State.seen` path

If the skeleton renders these correctly, the shape of the real pipeline is right.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from signal_core.contracts import (
    FetchOutcome,
    PayloadFormat,
    RawDocument,
    SourceConfig,
    State,
)
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now

# (story_key, headline, body, publisher, minutes_before_fetch)
_ARTICLES: list[tuple[str, str, str, str, int | None]] = [
    # A four-publisher syndication event — should collapse to one cluster.
    (
        "acq",
        "Northwind acquires Lumen Robotics for $2.4B",
        "Northwind said on Tuesday it would acquire Lumen Robotics in a cash deal valued at 2.4 "
        "billion dollars, its largest purchase to date.",
        "techcrunch.com",
        180,
    ),
    (
        "acq",
        "Lumen Robotics to be bought by Northwind in $2.4B deal",
        "Lumen Robotics will be acquired by Northwind for 2.4 billion dollars in cash, the "
        "companies confirmed Tuesday, marking Northwind's largest deal.",
        "theverge.com",
        150,
    ),
    (
        "acq",
        "Northwind to buy Lumen Robotics",
        "Northwind announced Tuesday it is acquiring Lumen Robotics for 2.4 billion "
        "dollars in cash.",
        "arstechnica.com",
        120,
    ),
    (
        "acq",
        "Northwind acquires Lumen Robotics for $2.4B",
        "Northwind said on Tuesday it would acquire Lumen Robotics in a cash deal valued at 2.4 "
        "billion dollars, its largest purchase to date.",
        "syndicated-wire.com",
        90,
    ),  # byte-identical reprint of the first
    # Two-publisher event.
    (
        "filing",
        "Perihelion Energy files S-1 ahead of IPO",
        "Perihelion Energy filed its S-1 with the SEC on Monday, disclosing revenue of 410 "
        "million dollars for the last fiscal year.",
        "reuters.com",
        300,
    ),
    (
        "filing",
        "Perihelion Energy sets IPO terms in S-1 filing",
        "Solar developer Perihelion Energy disclosed 410 million dollars in annual revenue in an "
        "S-1 filed with regulators Monday.",
        "arstechnica.com",
        260,
    ),
    # Singletons.
    (
        "cpi",
        "CPI rises 0.2% in July, below forecasts",
        "Consumer prices rose 0.2 percent in July, below the 0.3 percent economists expected, "
        "while core inflation held steady.",
        "reuters.com",
        420,
    ),
    (
        "chip",
        "Cassini Semiconductor unveils 2nm process",
        "Cassini Semiconductor said its 2nm manufacturing process will enter volume production "
        "next quarter.",
        "theverge.com",
        500,
    ),
    (
        "layoff",
        "Beacon Software cuts 900 roles",
        "Beacon Software will eliminate about 900 positions, roughly 7 percent of its workforce, "
        "according to an internal memo.",
        "techcrunch.com",
        540,
    ),
    # published_at absent — the RSS reality that §6.2's disagreement flag exists for.
    (
        "formd",
        "Halyard Labs raises $18M Series A",
        "Halyard Labs disclosed an 18 million dollar Series A in a Form D filing with the SEC.",
        "sec.gov",
        None,
    ),
    # published_at in the future — always a lie; we cannot fetch what does not exist.
    (
        "rates",
        "Central bank holds rates steady",
        "The central bank left its policy rate unchanged, citing continued progress on inflation.",
        "reuters.com",
        -120,
    ),
]


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    """Emit the fixture set, skipping anything `state` has already seen.

    Content is fixed so hashes are stable across runs; only `fetched_at` moves, which is
    what makes CI able to assert on cluster counts.
    """
    now = utc_now()
    documents: list[RawDocument] = []
    new_ids: list[str] = []

    for index, (story, title, body, publisher, offset) in enumerate(_ARTICLES):
        doc_id = f"fake-{index:03d}"
        if state.has_seen(doc_id):
            continue

        published_at: datetime | None = None if offset is None else now - timedelta(minutes=offset)
        url = f"https://{publisher}/{story}/{index}"
        article: dict[str, str | None] = {
            "id": doc_id,
            "story_key": story,  # ground truth, for the §7.1 eval set
            "title": title,
            "body": body,
            "publisher": publisher,
            "url": url,
            "published_at": published_at.isoformat() if published_at else None,
        }
        payload = json.dumps(article, sort_keys=True).encode("utf-8")

        documents.append(
            RawDocument(
                ingest_id=f"{config.source_id}-{now:%Y%m%dT%H%M%S}-{index:03d}",
                source_id=config.source_id,
                fetched_at=now,
                source_url=url,
                http_status=200,
                outcome=FetchOutcome.OK,
                etag=None,
                last_modified=None,
                content_hash=content_hash(payload),
                payload=payload,
                payload_format=PayloadFormat.JSON,
                latency_ms=1,
                byte_count=len(payload),
            )
        )
        new_ids.append(doc_id)

    outcome_state = state.remember(new_ids).model_copy(
        update={"last_success_at": now, "consecutive_failures": 0}
    )
    return documents, outcome_state
