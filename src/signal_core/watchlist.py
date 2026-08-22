"""The reader's watchlist. SPEC §7.4's `relevance` and `market_corroboration` inputs.

One surface, two consumers. `relevance` asks "is this cluster about something I care
about?"; `market_corroboration` asks "did the linked ticker move?" — and both questions are
answered from the same list, because two lists would eventually disagree about whether a
company is interesting, and the ranker would then score it both ways in the same run.

## Why the ids need no translation layer

`entities/dictionary.py` fixed the namespace before this module existed: **UPPERCASE is a
tradable ticker, `lower-kebab-case` is an entity without one.** So `tickers()` is a filter
over `companies`, not a join against `dim_entities` — a watchlist naming a private company
(`openai`) contributes nothing to fetch and still counts for relevance, and that falls out
of the id rather than out of a lookup that might come back empty.

## Loaded once

`lru_cache` on the loader, matching `entities/dictionary.py::load`. The file is small and
hand-edited between runs, never during one, and the ranker calls `is_relevant` once per
cluster per brief.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

WATCHLIST_PATH = Path(__file__).parent / "watchlist.toml"


@dataclass(frozen=True)
class Watchlist:
    """What the reader is interested in, as three flat collections.

    `companies` holds entity ids in the dictionary's namespace. `technologies` holds
    lowercase substrings. `macro_series` holds FRED ids and is inert until 4B — carried
    here so the watchlist stays one file (SPEC §7.4 names all three together).
    """

    companies: frozenset[str]
    technologies: tuple[str, ...]
    macro_series: tuple[str, ...]

    def tickers(self) -> frozenset[str]:
        """The tradable subset, which is exactly the uppercase ids.

        See `entities/dictionary.py`'s namespace note. `sources/stooq.py` fetches this set;
        a lowercase id like `openai` is a company with no listing and is silently not
        fetched, which is correct rather than an omission.
        """
        return frozenset(c for c in self.companies if c.isupper())

    def has_company(self, entity_id: str | None) -> bool:
        if not entity_id:
            return False
        return entity_id in self.companies

    def matched_technologies(self, *texts: str | None) -> tuple[str, ...]:
        """Which technology keywords appear across the given texts.

        Substring matching, case-insensitive: SPEC §7.4's relevance component is about
        subject matter, and `post-training` inside `post-training run` is a match while a
        token-based rule would miss it. Returns the matches rather than a bool so the
        ranker can put them in `score_components`' explanation rather than asserting an
        unexplained 1.0 — §7.4's requirement is that a ranking decision stays explainable
        after the fact.
        """
        haystack = " ".join(t.lower() for t in texts if t)
        if not haystack:
            return ()
        return tuple(k for k in self.technologies if k in haystack)


@lru_cache(maxsize=1)
def load(path: Path | None = None) -> Watchlist:
    raw = tomllib.loads((path or WATCHLIST_PATH).read_text(encoding="utf-8"))
    return Watchlist(
        companies=frozenset(raw.get("companies", [])),
        # Lowercased at load, so a capitalised entry in the file still matches. The file is
        # hand-edited and this is the kind of thing a hand-edit gets wrong silently.
        technologies=tuple(sorted({t.lower() for t in raw.get("technologies", [])})),
        macro_series=tuple(raw.get("macro_series", [])),
    )
