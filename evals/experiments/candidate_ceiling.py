#!/usr/bin/env python3
"""Why the alias index misses 20 of 54 linked mentions. ADR-0009 §2; 5.C carried.

ADR-0009 measured a hard bound and then reasoned from it: the alias index proposes the correct
entity for **34 of 54** linked mentions, so recall for any context-scoring rule over this
dictionary is capped at **0.630** against 0.611 shipped. It concluded that most of the gap
"needs `?itemDescription` **and** a wider candidate set".

That conclusion names two fixes without saying which mentions need which. This splits the 20
unreachable ones into causes, because they want different work:

- **absent** — the entity is not in the dictionary at all. Only a wider crawl reaches it
  (`MIN_SITELINKS`, the class closure, or a source that is not SEC or Wikidata).
- **present but unindexed** — the entity exists and the span does not reach it through any
  alias. An alias problem, not a notability one.

A description channel helps *neither* of those: descriptions disambiguate between candidates
that were proposed, and these mentions propose nothing. So this measurement decides how much of
ADR-0009 §2 is even addressable by the fix it names.

    uv run python evals/experiments/candidate_ceiling.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from signal_core.entities import dictionary as dict_module  # noqa: E402

EVALS = Path(__file__).resolve().parents[1]


def candidates(surface: str, dictionary) -> list[str]:
    """Same widening as `embed_entities.py::_candidates` — completes plus prefix matches."""
    tokens = dict_module.strip_legal_suffix(dict_module.normalize(surface))
    found: list[str] = []
    for start in range(len(tokens)):
        for end in range(len(tokens), start, -1):
            alias = dictionary.lookup(tokens[start:end])
            if alias is None:
                continue
            found.extend(alias.completes)
            found.extend(alias.starts[:5])
    seen, unique = set(), []
    for entity_id in found:
        if entity_id not in seen and entity_id in dictionary.entities:
            seen.add(entity_id)
            unique.append(entity_id)
    return unique


def main() -> int:
    mentions = [
        json.loads(line)
        for line in (EVALS / "entities" / "mentions.jsonl").read_text("utf-8").splitlines()
        if line.strip()
    ]
    dictionary = dict_module.load()
    linked = [m for m in mentions if m.get("entity_id")]
    print(f"{len(mentions)} mentions, {len(linked)} linked")
    print(f"dictionary: {len(dictionary.entities)} entities\n")

    causes: Counter[str] = Counter()
    unreachable: list[tuple[str, str, str]] = []
    for mention in linked:
        target = mention["entity_id"]
        if target in candidates(mention["surface_form"], dictionary):
            causes["reachable"] += 1
            continue
        cause = "present but unindexed" if target in dictionary.entities else "absent from dict"
        causes[cause] += 1
        unreachable.append((cause, mention["surface_form"], target))

    total = len(linked)
    print(f"ceiling: {causes['reachable']}/{total} = {causes['reachable'] / total:.3f}\n")
    for cause, count in causes.most_common():
        if cause != "reachable":
            print(f"  {cause:24} {count:>3}  ({count / total:.3f} of recall)")

    print("\nthe misses, in full:")
    for cause, surface, target in sorted(unreachable):
        print(f"  [{cause:21}] {surface!r:38} -> {target}")

    precision_errors(mentions, dictionary)
    return 0


def precision_errors(mentions: list[dict], dictionary) -> None:
    """The other half of the question: could a description channel fix what the resolver
    gets *wrong*, as opposed to what it never sees?

    A description disambiguates between candidates. So it can only help where the resolver
    linked to some entity and the labeled answer was a different one — an argmax it could
    have won. Where it linked something the labels call unlinked-entirely, or where the span
    is not a company at all, no description helps: there is no correct candidate to prefer.
    """
    from signal_core.entities.resolve import resolve

    fixable = wrong_kind = 0
    print("\nprecision errors, by whether a description could arbitrate:")
    for mention in mentions:
        linked = resolve(
            mention["surface_form"], mention.get("context") or "", dictionary=dictionary
        )
        predicted = linked.entity_id
        actual = mention.get("entity_id")
        if predicted is None or predicted == actual:
            continue
        candidate_ids = candidates(mention["surface_form"], dictionary)
        arbitrable = actual is not None and actual in candidate_ids
        fixable += arbitrable
        wrong_kind += not arbitrable
        verdict = "arbitrable" if arbitrable else "no correct candidate"
        print(f"  [{verdict:20}] {mention['surface_form']!r:34} {predicted} != {actual}")
    print(f"\n  {fixable} a description could arbitrate, {wrong_kind} it could not")


if __name__ == "__main__":
    raise SystemExit(main())
