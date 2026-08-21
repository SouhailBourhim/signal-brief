"""Finding the spans that might be companies. SPEC §7.2.

Detection, not resolution: this proposes candidate spans and says nothing about what they
refer to. `resolve.py` decides that, and the great majority of what turns up here is
correctly decided as nothing at all.

**This moved out of `evals/sample_mentions.py`, and the move is the point.** The 300 hand
labels were drawn against *these* spans, at *these* offsets. If the pipeline detected
mentions with a second implementation, the published precision/recall would describe spans
nobody labeled — the eval would be measuring a different system than the one running, which
is the exact failure `dedup.decide` exists as one function to avoid. The sampler now imports
from here.

**The heuristic is deliberately lexical and never consults the dictionary.** Sampling from
dictionary hits would publish recall-*given-a-candidate*: a number that looks like recall, is
always higher, and is blind to every company the dictionary has never heard of — which is
precisely SPEC §7.2's hard case. Keeping detection dictionary-free is what makes the recall
figure mean what it says.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Characters either side of a span, kept with it. 200 is what the labeled set carries, and
# `resolve` reads this window for the CIK a filing states and the full name a truncated span
# sits inside — so widening it would change resolutions, not just display.
CONTEXT_CHARS = 200

# Runs of capitalised words, optionally closed by a corporate suffix. Deliberately loose:
# over-generating is cheap (a human answers "not a company" in a second, and the resolver
# abstains) while a missed surface form is a mention that can never be resolved and therefore
# never counted.
PROPER_NOUN = re.compile(
    r"\b(?:[A-Z][\w&.'-]*)(?:[ ](?:of|for|and|de|van|der)?[ ]?[A-Z][\w&.'-]*){0,4}"
    r"(?:,?[ ](?:Inc|Corp|Corporation|Co|Ltd|LLC|LP|PLC|plc|NV|SA|AG|GmbH|Group|Holdings)\.?)?"
)
TICKER = re.compile(r"\$[A-Z]{1,5}\b")

# Words that start sentences constantly and are never a company on their own. A single-word
# candidate matching one of these is dropped; a multi-word one ("The Verge") survives.
SENTENCE_STARTERS = frozenset(
    [
        "The", "A", "An", "This", "That", "These", "Those", "It", "He", "She", "They", "We",
        "You", "I", "If", "When", "While", "After", "Before", "But", "And", "Or", "So",
        "Then", "Now", "Today", "Yesterday", "Tomorrow", "For", "To", "In", "On", "At", "By",
        "With", "From", "As", "Is", "Are", "Was", "Were", "Be", "Been", "Being", "Has",
        "Have", "Had", "Will", "Would", "Could", "Should", "May", "Might", "Its", "Their",
        "His", "Her", "Our", "Your", "My", "What", "Why", "How", "Where", "Who", "Which",
        "There", "Here", "All", "Some", "Many", "Most", "More", "Less", "Other", "New",
        "Old", "First", "Last", "Next", "One", "Two", "Three", "Show", "Ask", "Tell", "Get",
        "Make", "Use",
    ]
)  # fmt: skip


@dataclass(frozen=True)
class Mention:
    """One candidate span, before anything is decided about it."""

    surface_form: str
    char_start: int
    char_end: int


def mention_text(title: str | None, body_text: str | None) -> str:
    """`title + "\\n" + body_text`, which is what `char_start` indexes into.

    Pinned here rather than assembled at each call site because the offsets in
    `evals/entities/mentions.jsonl` are offsets into *this* string, and `silver.articles` is
    immutable while a cleaning rule is not. A span located against a cleaned copy would point
    somewhere else the first time cleaning changed.
    """
    return f"{title or ''}\n{body_text or ''}"


def detect(text: str) -> list[Mention]:
    """Every proper-noun-ish span, in the order they appear."""
    found: list[Mention] = []
    for match in TICKER.finditer(text):
        found.append(Mention(match.group(), match.start(), match.end()))
    for match in PROPER_NOUN.finditer(text):
        surface = match.group().strip().rstrip(",")
        if not surface or len(surface) < 2:
            continue
        # Single capitalised word that is just a sentence opening: not worth a judgement.
        if " " not in surface and surface in SENTENCE_STARTERS:
            continue
        # All-caps single tokens are usually acronyms in headlines (AI, CEO, SEC); keep the
        # ones long enough to plausibly be a name, drop the two-letter noise.
        if surface.isupper() and len(surface) <= 2:
            continue
        found.append(Mention(surface, match.start(), match.start() + len(surface)))
    return found


def context_window(text: str, start: int, end: int) -> str:
    """The ±`CONTEXT_CHARS` around a span, newlines flattened, elided at the edges."""
    lo = max(0, start - CONTEXT_CHARS)
    hi = min(len(text), end + CONTEXT_CHARS)
    return ("…" if lo else "") + text[lo:hi].replace("\n", " ") + ("…" if hi < len(text) else "")


def mention_id(article_id: str, char_start: int) -> str:
    """`<article_id>:<char_start>` — SPEC §9's `mention_id`, and the labeled set's key.

    An article plus an offset is unique by construction and stable across re-runs, which is
    what lets a resolution be replaced on re-resolve rather than duplicated.
    """
    return f"{article_id}:{char_start}"
