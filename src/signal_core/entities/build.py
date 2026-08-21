"""Build `warehouse/entities/dictionary.json` from SEC, Wikidata and a frequency list.

    uv run python -m signal_core.entities.build

The only part of entity resolution that touches the network, and it runs by hand rather than
on a schedule: the output is committed, so a rebuild is a reviewable diff and the published
precision/recall stays reproducible against a pinned dictionary (see `dictionary.py`).

Three sources, each doing one job:

**SEC `company_tickers.json`** — the tradable universe, 10,387 rows of `(cik, ticker,
title)`. Authoritative for exactly the thing a ticker id claims, and free of the modeling
guesswork the next source has.

**Wikidata** — the companies SEC has never heard of, because they are private (`OpenAI`,
`Anthropic`), foreign-listed, or not companies at all in SEC's sense (`The Verge`). It also
supplies `P749 parent organization`, which is what turns `GitHub` into `MSFT` rather than
into a slug — SPEC §7.2's "subsidiaries rolling to parents".

**A frequency-ranked English word list** — the negative evidence. Company names collide with
everyday words constantly (`Apple`, `Block`, `Meta`, `Windows`, `Progress`, `Momentum`), and
a resolver with no notion of which strings are ordinary English will link a river to a
retailer. Ranked rather than a flat set, because the rank is a fitted threshold in
`resolve.py`: `xerox` sits at 8,890 and `momentum` at 9,018, so where the line falls is a
measurement, not a taste.

## Wikidata will not give you "all companies", and this is why the class list is explicit

The correct query is `?item wdt:P31/wdt:P279* wd:Q4830453` — every instance of anything that
is transitively a kind of business. The public WDQS endpoint answers it with a **504 after
60 seconds**, measured 2026-08-21, at every notability floor tried. So the subclass closure
is materialized in two cheap steps instead: fetch the classes (a class-only traversal, small
and fast), then fetch instances in chunks of those classes. Same answer, several requests.

The floor on sitelinks is what keeps it to a dictionary rather than a dump. Every company
Wikidata knows, including a five-person consultancy with one citation, is a set whose noise
would swamp the signal: an alias index is only as precise as its rarest junk entry.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from signal_core.config import settings
from signal_core.entities import dictionary as dict_module
from signal_core.entities.dictionary import Entity, slug
from signal_core.timeutil import utc_now

SEC_URL = "https://www.sec.gov/files/company_tickers.json"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
# first20hours/google-10000-english: the 10,000 commonest English words in Google's
# trillion-word n-gram corpus, in descending frequency order. Pinned to a tag rather than
# `master` so a rebuild years from now gets the same list this dictionary was built from.
WORDS_URL = (
    "https://raw.githubusercontent.com/first20hours/google-10000-english/"
    "aab40297bf2bf72ebba8fdf02f975834391ae81a/google-10000-english.txt"
)

# Wikidata's root class for "business". The closure below it is what the two-step fetch
# materializes.
BUSINESS_CLASS = "Q4830453"

# A company with fewer Wikipedia editions than this is not one a daily brief will mention,
# and every one of them is another string that can be mistaken for an ordinary word. Chosen
# on coverage rather than fitted: at 5, the floor admits `Substack` (24), `Hugging Face`
# (29) and `EncroChat` (17) — private companies the labeled set actually contains — while
# holding the download to ~28k rows. It is a knob on *dictionary size*, not on the decision,
# so it does not belong in `evals/fit_thresholds.py` with the constants that are.
MIN_SITELINKS = 5

# WDQS is a shared free service with a published etiquette: one query at a time, a real
# User-Agent, and back off when told to. Being rate-limited mid-build is recoverable; being
# blocked is not.
WDQS_PAUSE_SECONDS = 2.0
WDQS_RETRIES = 5
# 50 classes per request, not 120. The alias join is what makes these queries expensive, and
# at 120 the endpoint answered 502 partway through a run. Smaller chunks are more requests
# and a build that finishes.
CLASS_CHUNK = 50
# Everything WDQS says when it is overloaded rather than when the query is wrong. 502 and 503
# belong here for the same reason 504 does: measured, from a build that died on one.
WDQS_RETRYABLE = frozenset({429, 500, 502, 503, 504})


def _client(timeout: float = 120.0) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    )


def fetch_sec(client: httpx.Client) -> list[Entity]:
    """The tradable universe. One entity per CIK, at its primary listing.

    SEC's file is one row per *ticker*, so a company with share classes, ETNs or structured
    notes appears many times — `BANK OF MONTREAL /CAN/` 32 times. The file is ordered by
    prominence, so the lowest index for a CIK is the common share class, which is what a
    brief means by "the ticker". See `dictionary.py` for the four hand-labeled mentions that
    check this.
    """
    response = client.get(SEC_URL)
    response.raise_for_status()
    rows = response.json()

    primary: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, row in sorted(rows.items(), key=lambda kv: int(kv[0])):
        cik = str(row["cik_str"]).zfill(10)
        if cik not in primary:
            primary[cik] = (int(index), row)
    return [
        Entity(
            entity_id=row["ticker"].upper(),
            canonical_name=row["title"],
            entity_type="public",
            source="sec",
            ticker=row["ticker"].upper(),
            cik=cik,
            rank=index,
        )
        for cik, (index, row) in primary.items()
    ]


def _sparql(client: httpx.Client, query: str) -> list[dict[str, Any]]:
    """One SPARQL query, with the backoff WDQS asks for."""
    for attempt in range(WDQS_RETRIES):
        response = client.get(
            WIKIDATA_SPARQL,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
        )
        if response.status_code == 200:
            return list(response.json()["results"]["bindings"])
        # 429 is the rate limiter, 5xx the endpoint being overloaded or the query timing out
        # server-side. All are worth another try after a wait; anything else is a bug in the
        # query and retrying it just annoys a free shared service.
        if response.status_code not in WDQS_RETRYABLE:
            response.raise_for_status()
        wait = WDQS_PAUSE_SECONDS * (2**attempt)
        print(f"  WDQS {response.status_code}, retrying in {wait:.0f}s", file=sys.stderr)
        time.sleep(wait)
    raise RuntimeError(f"WDQS did not answer after {WDQS_RETRIES} attempts")


def _business_classes(client: httpx.Client) -> list[str]:
    """Everything that is transitively a kind of business. Classes only — a few thousand
    rows, and the half of the closure the endpoint will actually compute."""
    rows = _sparql(client, f"SELECT ?c WHERE {{ ?c wdt:P279* wd:{BUSINESS_CLASS} . }}")
    return [row["c"]["value"].rsplit("/", 1)[-1] for row in rows]


def _qid(binding: dict[str, Any] | None) -> str | None:
    return binding["value"].rsplit("/", 1)[-1] if binding else None


def fetch_wikidata(client: httpx.Client) -> tuple[list[Entity], dict[str, str]]:
    """Notable businesses SEC does not list, plus their parent links.

    Returns entities keyed by slug and a `qid -> parent qid` map, which is resolved to
    entity ids after the fact — a subsidiary's parent is only useful once both are known,
    and the parent may arrive in a later chunk.
    """
    classes = _business_classes(client)
    print(f"  {len(classes)} business subclasses")

    seen: dict[str, dict[str, Any]] = {}
    parents: dict[str, str] = {}
    for start in range(0, len(classes), CLASS_CHUNK):
        chunk = classes[start : start + CLASS_CHUNK]
        values = " ".join(f"wd:{qid}" for qid in chunk)
        query = f"""
        SELECT ?item ?itemLabel ?sitelinks ?parent
               (GROUP_CONCAT(DISTINCT ?altLabel; separator="|") AS ?aliases) WHERE {{
          VALUES ?class {{ {values} }}
          ?item wdt:P31 ?class .
          ?item wikibase:sitelinks ?sitelinks .
          FILTER(?sitelinks >= {MIN_SITELINKS})
          OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(lang(?altLabel) = "en") }}
          OPTIONAL {{ ?item wdt:P749 ?parent }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} GROUP BY ?item ?itemLabel ?sitelinks ?parent
        """
        rows = _sparql(client, query)
        for row in rows:
            qid = _qid(row["item"])
            if qid is None or qid in seen:
                continue
            seen[qid] = row
            parent = _qid(row.get("parent"))
            if parent:
                parents[qid] = parent
        print(
            f"  classes {start:>5}-{start + len(chunk):<5} {len(rows):>6} rows, "
            f"{len(seen)} entities so far"
        )
        time.sleep(WDQS_PAUSE_SECONDS)

    entities = []
    qid_to_id = {}
    for qid, row in seen.items():
        label = row["itemLabel"]["value"]
        # An unlabeled item comes back as its own QID. That is a Wikidata gap, not a name.
        if label == qid or not label.strip():
            continue
        entity_id = slug(label)
        if not entity_id:
            continue
        qid_to_id[qid] = entity_id
        raw = row.get("aliases", {}).get("value", "")
        entities.append(
            Entity(
                entity_id=entity_id,
                canonical_name=label,
                entity_type="private",
                source="wikidata",
                aliases=tuple(sorted({a.strip() for a in raw.split("|") if a.strip()})),
            )
        )
    return entities, {
        qid_to_id[child]: qid_to_id[parent]
        for child, parent in parents.items()
        if child in qid_to_id and parent in qid_to_id
    }


def fetch_common_words(client: httpx.Client) -> list[str]:
    response = client.get(WORDS_URL)
    response.raise_for_status()
    return [line.strip() for line in response.text.splitlines() if line.strip()]


def _merge(sec: list[Entity], wikidata: list[Entity], parents: dict[str, str]) -> list[Entity]:
    """SEC wins every collision, and subsidiaries inherit a tradable parent's id.

    Two collisions to settle, in this order:

    1. **A Wikidata company that SEC also lists.** `Xerox` is in both. SEC's row carries the
       ticker and the CIK, so it is strictly more useful, and keeping both would put two ids
       on one company — the failure `dim_entities` exists to prevent.
    2. **A subsidiary of a tradable parent.** Wikidata says `GitHub`'s parent organization is
       Microsoft; Microsoft resolves to `MSFT`; so `GitHub` resolves to `MSFT` too. This is
       SPEC §7.2's rollup rule, and it is the one place the dictionary makes an inference
       rather than a lookup — recorded as `parent_entity_id` so it is visible afterwards.

       The rollup stops at the first tradable ancestor and only follows links inside the
       snapshot, so a chain that leaves the dictionary leaves the entity as itself rather
       than guessing.
    """
    by_name = {
        " ".join(dict_module.strip_legal_suffix(dict_module.normalize(e.canonical_name))): e
        for e in sec
    }
    tradable_slug = {slug(e.canonical_name): e.entity_id for e in sec}

    merged = list(sec)
    for entity in wikidata:
        name_key = " ".join(
            dict_module.strip_legal_suffix(dict_module.normalize(entity.canonical_name))
        )
        if name_key in by_name:
            continue
        parent_id = parents.get(entity.entity_id)
        tradable_parent = tradable_slug.get(parent_id or "")
        merged.append(
            Entity(
                entity_id=tradable_parent or entity.entity_id,
                canonical_name=entity.canonical_name,
                entity_type="public" if tradable_parent else "private",
                source="wikidata",
                ticker=tradable_parent,
                parent_entity_id=tradable_parent,
                aliases=entity.aliases,
            )
        )
    return merged


def run(out_path: Path, *, skip_wikidata: bool = False) -> int:
    started = utc_now()
    with _client() as client:
        print("SEC company_tickers.json …")
        sec = fetch_sec(client)
        print(f"  {len(sec)} tradable entities")

        wikidata: list[Entity] = []
        parents: dict[str, str] = {}
        if not skip_wikidata:
            print("Wikidata …")
            wikidata, parents = fetch_wikidata(client)
            print(f"  {len(wikidata)} entities, {len(parents)} parent links")

        print("English frequency list …")
        words = fetch_common_words(client)
        print(f"  {len(words)} words")

    entities = _merge(sec, wikidata, parents)
    built = dict_module.build(
        entities,
        common_words=words,
        built_at=started.isoformat(),
        sources={
            "sec": {"url": SEC_URL, "entities": len(sec)},
            "wikidata": {
                "endpoint": WIKIDATA_SPARQL,
                "root_class": BUSINESS_CLASS,
                "min_sitelinks": MIN_SITELINKS,
                "entities": len(wikidata),
            },
            "common_words": {"url": WORDS_URL, "words": len(words)},
        },
    )
    dict_module.write(built, out_path)
    print(
        f"\n{len(built.entities)} entities, {len(built.aliases)} aliases -> {out_path} "
        f"({out_path.stat().st_size / 1e6:.1f} MB)"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=dict_module.DEFAULT_PATH)
    parser.add_argument(
        "--skip-wikidata",
        action="store_true",
        help="SEC and the word list only — a fast rebuild when WDQS is unavailable",
    )
    args = parser.parse_args(argv)
    return run(args.out, skip_wikidata=args.skip_wikidata)


if __name__ == "__main__":
    raise SystemExit(main())
