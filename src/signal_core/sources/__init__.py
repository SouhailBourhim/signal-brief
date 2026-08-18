"""Source registry. SPEC §6.1.

`REGISTRY` maps source_id to a Poller. The Lambda handler looks a source up here by its
SOURCE_ID environment variable, which is how N Lambda functions run from one artifact.
"""

from __future__ import annotations

from signal_core.contracts import Poller
from signal_core.sources.fake import poll as fake_poll

REGISTRY: dict[str, Poller] = {
    "fake": fake_poll,
}


def get_poller(source_id: str) -> Poller:
    try:
        return REGISTRY[source_id]
    except KeyError:
        raise KeyError(f"unknown source {source_id!r}; registered: {sorted(REGISTRY)}") from None
