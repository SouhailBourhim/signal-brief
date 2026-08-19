"""Source registry. SPEC §6.1.

`REGISTRY` maps source_id to a Poller. The Lambda handler looks a source up here by its
SOURCE_ID environment variable, which is how N Lambda functions run from one artifact.
"""

from __future__ import annotations

from signal_core.contracts import Poller
from signal_core.sources.edgar import poll as edgar_poll
from signal_core.sources.edgar_formd import poll as edgar_formd_poll
from signal_core.sources.fake import poll as fake_poll
from signal_core.sources.hackernews import poll as hackernews_poll
from signal_core.sources.rss_ars import poll as rss_ars_poll
from signal_core.sources.rss_tech import poll as rss_tech_poll
from signal_core.sources.rss_verge import poll as rss_verge_poll

REGISTRY: dict[str, Poller] = {
    "fake": fake_poll,
    "hackernews": hackernews_poll,
    "edgar": edgar_poll,
    "rss_tech": rss_tech_poll,
    # Phase 2 (SPEC §3). `rss_ars` is source #6 — the one that tests §3's 30-minute claim.
    "edgar_formd": edgar_formd_poll,
    "rss_verge": rss_verge_poll,
    "rss_ars": rss_ars_poll,
}


def get_poller(source_id: str) -> Poller:
    try:
        return REGISTRY[source_id]
    except KeyError:
        raise KeyError(f"unknown source {source_id!r}; registered: {sorted(REGISTRY)}") from None
