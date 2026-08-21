"""The entity dictionary: names in, canonical ids out. SPEC §7.2, §9.

A committed snapshot, not a live lookup. `warehouse/entities/dictionary.json` is built by
`build.py` from SEC and Wikidata and then read offline by everything else — the resolver,
the eval scorer, the Spark job. Three reasons it is a file rather than a fetch:

1. `make eval` runs in CI, and no test in this repo touches the network.
2. A published precision/recall figure is only reproducible if the dictionary it was
   measured against is pinned. "Precision 0.9 against whatever SEC served that morning" is
   not a measurement anybody can check.
3. SEC renames companies. A snapshot with a `built_at` is the raw material `dim_entities`
   turns into SCD2 rows; a live lookup would silently rewrite history.

## The id namespace carries a claim

`evals/entities/README.md` fixed this before any of this code existed, and it is load-
bearing rather than cosmetic: **UPPERCASE is a tradable ticker, `lower-kebab-case` is an
entity without one.** SPEC §7.4's market-corroboration component asks "did the linked ticker
move?", and a namespace where that question is answerable by looking at the id — rather than
by a join that might come back empty — is one that cannot drift out of sync with itself.

## Aliases are prefixes, because that is how companies are actually named

An entity contributes every **token prefix** of its legal name, suffix stripped: `Getty
Images Holdings, Inc.` yields `getty`, `getty images`, `getty images holdings`. Prose says
"Getty Images" and the SEC says "Getty Images Holdings, Inc."; a dictionary keyed only on
full legal names matches neither half of that sentence.

Prefixes collide, so each alias records both the entities it *completes* and the entities it
merely *starts*. `apple` completes `Apple Inc.` and starts `Apple Hospitality REIT, Inc.` —
one complete match wins over any number of prefix matches, and that ordering is what keeps
`AAPL` from being ambiguous with a hotel REIT. Two complete matches is genuine ambiguity and
resolves to nothing, per SPEC §7.2's floor.

## One company, many tickers

2,393 of SEC's 10,387 rows are a duplicate title: `BANK OF MONTREAL /CAN/` appears 32 times,
once per structured note it issues. The primary listing is the one a brief means, and SEC's
own file ordering identifies it — the file is ordered by prominence (index 0 is NVDA, 1 is
AAPL), so the lowest index for a CIK is its common share class. Verified against four
independently hand-labeled mentions: `AEG` not `AEGOF`, `BMO` not `FNGD`, `CMCSA` not `CCZ`,
`XRX` not `XRXDW`. That is SEC's editorial judgment being reused rather than ours being
invented.
"""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

# Legal-form suffixes, dropped from a name before it becomes an alias. Everything from the
# first one onward goes, which is what reduces `JS VENTURE FUND LLC SERIES` to `js venture
# fund` rather than leaving the trailing `series` on it.
#
# Words that look like suffixes and are deliberately absent: `group`, `holdings`, `fund`,
# `bank`, `partners`, `companies`. They are part of the name a person would say, and the
# hand labels agree — `SpotWise Data Group LLC` was labeled `spotwise-data-group`, keeping
# the `group` and dropping only the `llc`.
LEGAL_SUFFIXES = frozenset(
    [
        "inc",
        "incorporated",
        "corp",
        "corporation",
        "co",
        "company",
        "ltd",
        "limited",
        "llc",
        "lp",
        "llp",
        "lllp",
        "plc",
        "nv",
        "bv",
        "sa",
        "sas",
        "ag",
        "gmbh",
        "kgaa",
        "se",
        "ab",
        "asa",
        "oyj",
        "spa",
        "srl",
        "pty",
        "kk",
        "pte",
        "sarl",
        "aps",
        "as",
    ]
)

# EDGAR staples a state or country of incorporation onto its titles — `BANK OF MONTREAL
# /CAN/`, `GETTY REALTY CORP /MD/`. It is registry metadata, not part of the name.
_STATE_MARKER = re.compile(r"/[A-Za-z]{2,3}/")
# Everything that is not a letter, digit or ampersand separates tokens. `&` survives because
# `H&R BLOCK` is one word to a reader and splitting it invents two meaningless tokens.
_SEPARATOR = re.compile(r"[^a-z0-9&]+")
# Possessives, removed *before* tokenizing rather than after. The apostrophe is itself a
# separator, so `Comcast's` splits into `comcast` and a stray `s` — which then pads every
# n-gram containing it and shifts the offsets the buried-match penalty reads.
_POSSESSIVE = re.compile("['\u2019]s\\b")


def normalize(name: str) -> tuple[str, ...]:
    """A name reduced to comparable tokens. Case, punctuation and registry markers out."""
    lowered = _POSSESSIVE.sub("", _STATE_MARKER.sub(" ", name or "").lower())
    return tuple(token for token in _SEPARATOR.split(lowered) if token)


def strip_legal_suffix(tokens: tuple[str, ...]) -> tuple[str, ...]:
    """Tokens up to the first legal-form suffix.

    Never returns empty: a name that *is* a suffix (`CO`, an actual EDGAR filer name) keeps
    its tokens rather than reducing to nothing and matching everything.
    """
    for index, token in enumerate(tokens):
        if token in LEGAL_SUFFIXES:
            return tokens[:index] if index else tokens
    return tokens


def has_legal_suffix(tokens: tuple[str, ...]) -> bool:
    """Whether a name declares a legal form — `Lyntris Inc.`, `Mishpacha Fund, LP`.

    This is the licence to mint an id for a company the dictionary has never heard of. It is
    also, in this corpus, the line between a company and a person: EDGAR Form 4 and 144
    filers are individuals (`GEE DAVID NICHOLAS`, `Turco Christopher Edward`) and they are
    the largest single source of company-shaped spans, which nothing but the absence of a
    legal form distinguishes from a private company nobody has indexed.
    """
    return any(token in LEGAL_SUFFIXES for token in tokens)


def slug(name: str) -> str:
    """The `lower-kebab-case` id for an entity with no ticker.

    Deterministic and derived from the name, so two runs mint the same id for the same
    company and `dim_entities` does not accumulate synonyms of itself.
    """
    return "-".join(strip_legal_suffix(normalize(name)))


@dataclass(frozen=True)
class Entity:
    """One resolvable company. Mirrors SPEC §9's `dim_entities` minus the SCD2 columns,
    which are added when a snapshot is loaded into the table rather than carried here — a
    dictionary describes what is true now, and validity intervals are what the table adds."""

    entity_id: str
    canonical_name: str
    entity_type: str  # "public" | "private"
    source: str  # "sec" | "wikidata"
    ticker: str | None = None
    cik: str | None = None
    # Position in SEC's own file, which is ordered by prominence — index 0 is NVDA, 1 is
    # AAPL. It is the tiebreak when a prefix belongs to several companies: `meta` starts
    # `Meta Platforms`, `Metagenomi` and `Metallus`, and one of those is what a news brief
    # means. Reusing SEC's editorial ordering is the same move that picks `AEG` over
    # `AEGOF`; absent (Wikidata entities), it sorts last.
    rank: int | None = None
    # Set when this entity is not itself tradable but rolls up to one that is — SPEC §7.2's
    # "subsidiaries rolling to parents". `GitHub` resolves to `MSFT`, not to `github`.
    parent_entity_id: str | None = None
    # Other names for the same company — SPEC §7.2's "plus Wikidata aliases". Indexed exactly
    # like the canonical name, prefixes and all, because an alias is a name and gets shortened
    # in prose the same way. They are the only way a dictionary keyed on legal names reaches
    # `Google` for `Google LLC`'s parent or `Hugging Face` under its one-word spelling.
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class Alias:
    """One lookup key, and the difference between completing a name and merely starting it.

    Kept as two sets rather than one ranked list because the tie-break is categorical: a
    complete match beats any number of prefix matches, and two complete matches are
    ambiguous however many prefix matches sit behind them.
    """

    completes: tuple[str, ...] = ()
    starts: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dictionary:
    entities: dict[str, Entity] = field(default_factory=dict)
    aliases: dict[str, Alias] = field(default_factory=dict)
    # CIK -> entity_id. The highest-precision channel this corpus offers: an EDGAR filing
    # states its filer's CIK, so linking one is reading an identifier rather than inferring
    # an entity.
    by_cik: dict[str, str] = field(default_factory=dict)
    # The N most frequent English words, by descending frequency rank. A single-token alias
    # that is also an everyday word — `apple`, `block`, `meta`, `windows` — cannot be linked
    # on the strength of the word alone. See `resolve.py`, where the cutoff is fitted.
    common_words: dict[str, int] = field(default_factory=dict)
    built_at: str = ""
    sources: dict[str, Any] = field(default_factory=dict)

    def lookup(self, tokens: tuple[str, ...]) -> Alias | None:
        return self.aliases.get(" ".join(tokens))

    def word_rank(self, token: str) -> int | None:
        """Frequency rank of a token in everyday English, or None if it is not one."""
        return self.common_words.get(token)


def build(
    entities: list[Entity],
    *,
    common_words: list[str] | None = None,
    built_at: str = "",
    sources: dict[str, Any] | None = None,
) -> Dictionary:
    """Index a list of entities into a dictionary. Pure — `build.py` does the fetching."""
    aliases: dict[str, tuple[list[str], list[str]]] = {}
    for entity in entities:
        for name in (entity.canonical_name, *entity.aliases):
            tokens = strip_legal_suffix(normalize(name))
            for length in range(1, len(tokens) + 1):
                key = " ".join(tokens[:length])
                completes, starts = aliases.setdefault(key, ([], []))
                (completes if length == len(tokens) else starts).append(entity.entity_id)
    # First writer wins. `build.py` puts SEC first, so a Wikidata row can never displace a
    # ticker's canonical name, CIK or rank. This used to be a plain dict comprehension, and
    # a single subsidiary rollup silently took AEGON's CIK out of `by_cik` — see `_merge`.
    by_id: dict[str, Entity] = {}
    for entity in entities:
        by_id.setdefault(entity.entity_id, entity)
    return Dictionary(
        entities=by_id,
        aliases={
            key: Alias(completes=tuple(sorted(set(c))), starts=tuple(sorted(set(s))))
            for key, (c, s) in aliases.items()
        },
        by_cik={e.cik: e.entity_id for e in by_id.values() if e.cik},
        common_words={word: rank for rank, word in enumerate(common_words or [])},
        built_at=built_at,
        sources=sources or {},
    )


def to_json(dictionary: Dictionary) -> str:
    """The on-disk form. Entities and the word list only — the alias index is derived, and
    storing it would triple the file while letting it drift out of step with `build`."""
    return json.dumps(
        {
            "built_at": dictionary.built_at,
            "sources": dictionary.sources,
            "common_words": [
                word for word, _ in sorted(dictionary.common_words.items(), key=lambda kv: kv[1])
            ],
            "entities": [
                {k: v for k, v in vars(entity).items() if v is not None and v != ()}
                for entity in dictionary.entities.values()
            ],
        },
        separators=(",", ":"),
        sort_keys=False,
    )


def from_json(text: str) -> Dictionary:
    payload = json.loads(text)
    return build(
        [
            Entity(**{**row, "aliases": tuple(row.get("aliases", ()))})
            for row in payload["entities"]
        ],
        common_words=payload.get("common_words", []),
        built_at=payload.get("built_at", ""),
        sources=payload.get("sources", {}),
    )


# Repo-relative, matching how `Settings.data_root` and `out_root` are already declared. The
# snapshot is source, not runtime state, so it lives with the warehouse definitions it seeds
# (SPEC §13) rather than under the gitignored `data/`.
#
# Gzipped, and nothing is lost by it: the file is emitted as one line of separator-free JSON,
# so a line diff was never going to be readable, and the thing a reviewer actually checks is
# `built_at` and the entity counts in `sources` — which `signal report` prints and a diff of
# 8 MB of JSON does not. It also keeps the snapshot inside the repo's 512 KB large-file
# guard rather than requiring that guard to be loosened for one generated artifact.
DEFAULT_PATH = Path("warehouse/entities/dictionary.json.gz")


def write(dictionary: Dictionary, path: Path) -> None:
    """Write a snapshot, gzipped or not according to the path's suffix.

    Plain `.json` stays supported because reading one by eye during a rebuild is genuinely
    useful, and a format that can only be opened by its own loader is a format that stops
    being checked.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = to_json(dictionary).encode("utf-8")
    if path.suffix == ".gz":
        # mtime=0 so rebuilding an unchanged dictionary produces an identical file rather
        # than a diff that is only a timestamp.
        path.write_bytes(gzip.compress(payload, compresslevel=9, mtime=0))
    else:
        path.write_bytes(payload)


@lru_cache(maxsize=4)
def load(path: Path | None = None) -> Dictionary:
    """The committed snapshot, parsed once per process.

    Cached because the Spark job resolves tens of thousands of mentions against it and
    re-reading 10k entities per call is the sort of thing that turns a 20-second job into a
    coffee break.
    """
    resolved = path or DEFAULT_PATH
    if not resolved.exists():
        raise FileNotFoundError(
            f"no entity dictionary at {resolved}. Build one with "
            "`uv run python -m signal_core.entities.build` (it fetches SEC and Wikidata), "
            "or pass a path."
        )
    raw = resolved.read_bytes()
    if resolved.suffix == ".gz":
        raw = gzip.decompress(raw)
    return from_json(raw.decode("utf-8"))
