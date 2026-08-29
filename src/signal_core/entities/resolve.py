"""Mention -> entity, or nothing at all. SPEC §7.2.

`resolve` is the single decision, the way `dedup.decide` is for same-story. The Spark job
calls it and `evals/score.py` calls it; neither reimplements it, so the published
precision/recall describes the system that actually ships.

## Abstaining is an answer, not a failure

SPEC §7.2: "a confidence floor below which a mention is left **unlinked rather than
guessed**". The hand-labeled set makes the stakes concrete — **246 of 300 mentions are
correctly unlinked**, because a proper-noun heuristic over real feeds yields `Filed`,
`AccNo`, `Show HN`, `Certain Officers`, and the surname-first names of every EDGAR Form 4
filer. A resolver rewarded only for linking would learn to link all of them.

## Four channels, most certain first

1. **CIK** — the span is a filer name and the context states its CIK: `BofA Finance LLC
   (0001682472)`. Not inference at all, just reading an identifier the filing supplied. 30
   of the 300 labeled mentions carry one.
2. **Name** — the span, or an n-gram inside it, is the complete name of a dictionary entity.
   `Comcast` completes `COMCAST CORP`.
3. **Prefix** — the n-gram starts a longer name: `Getty Images` starts `Getty Images
   Holdings, Inc.`. Weaker, because a prefix can start several names.
4. **Mint** — the span declares a legal form (`Mishpacha Fund, LP`) and no dictionary entry
   claims it, so it becomes a `lower-kebab-case` id of its own. SPEC §7.2's "private
   companies with no ticker still get an entity_id"; `dictionary.slug` makes it deterministic
   so two runs mint the same id rather than two synonyms.

Everything else abstains, and `Resolution.reason` records *which* way it abstained so the
failure modes can be read off a run instead of guessed at.

## The common-word problem, and what this deliberately does not do

`Apple`, `Block`, `Meta`, `Windows` and `Amazon` are company names and ordinary English
words, and the labels contain both readings — `Meta` the company and `Amazon` the river, in
the same 300. Telling them apart from the word alone is impossible; SPEC §7.2's answer is
"cosine similarity between article context and entity description embeddings", and Phase 3
took a documented decision not to add an ML dependency yet (`docs/runbooks/phase-3.md`).

So this resolver does the lexical half honestly: a single-token alias that is also an
everyday word cannot carry a link on its own, and links only when the context **names the
entity in full somewhere else** (`Meta Platforms` near `Meta`). Where it does not, the answer
is unlinked. That is a known, measured recall gap with a named fix, which is a better thing
to publish than a guess that happens to score well on whichever half of the corpus was
sampled.

## Why so few constants are fitted

`CONFIDENCE_FLOOR` and `COMMON_WORD_RANK` are fitted by `evals/fit_thresholds.py`; the
per-channel confidences are not. With 54 positive mentions, fitting eight weights would
choose them from noise — the same reasoning that keeps `MIN_SIMHASH_TOKENS` out of the dedup
grid. The confidences below are an **ordering of evidence kinds**, stated once and left
alone; the floor is what turns that ordering into a decision, and the floor is measured.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from signal_core.dedup import FEED_BOILERPLATE, strip_boilerplate
from signal_core.entities import dictionary as dict_module
from signal_core.entities.dictionary import Dictionary

# --- fitted (`evals/fit_thresholds.py --set entities`) ------------------------------------

# Below this, a mention is left unlinked. SPEC §7.2's confidence floor, and the one knob that
# decides which of the channels below are trusted at all. Fitted at 0.72, which sits just
# above `CONFIDENCE_PREFIX` minus the single-token penalty (0.60) — so a one-word prefix match
# does not link on its own, and `Getty Images` (two words, 0.70) does not either while a
# complete name (0.80 single, 0.90 multi) and everything stronger does.
CONFIDENCE_FLOOR = 0.72

# A single-token alias whose frequency rank in everyday English is better (lower) than this
# is a word first and a company second, and needs the context to name the entity in full
# before it links. Landmarks either side: `windows` ranks 453, `apple` 1,642, `meta` 5,164 —
# against `xerox` at 8,890 and `asana`, `comcast`, `substack` nowhere in the list at all.
#
# **Held fixed, not fitted, and the measurement says both halves of that.** At the fitted
# floor every value from 2,000 to 10,000 scores identically (train 0.900/0.667, held out
# 0.833/0.556), so the labeled set cannot choose between them and the stated rule does:
# link less on equal evidence, which means the whole list. What the set *can* see is whether
# the channel earns its place at all — at 0, with the word list switched off entirely,
# held-out precision falls to 0.727.
#
# That last row is also a clean illustration of why the fitting is split into a train half
# and a held-out one. Switching the channel off scores *better* on train (0.905 against
# 0.900) and worse on the half nobody fitted against. A procedure that looked only at train
# would have deleted it.
COMMON_WORD_RANK = 10000

# --- stated, not fitted -------------------------------------------------------------------

# An identifier the source supplied. Nothing else in this corpus is this certain.
CONFIDENCE_CIK = 1.0
# The span names the entity exactly.
CONFIDENCE_NAME = 0.90
# The span starts the entity's name. `Getty Images` for `Getty Images Holdings, Inc.` —
# right almost always, but a prefix genuinely belongs to whatever follows it.
CONFIDENCE_PREFIX = 0.70
# A legal form, and no entry to match it against. The suffix is the evidence: it is what
# separates `Mishpacha Fund, LP` from `Turco Christopher Edward`.
CONFIDENCE_MINTED = 0.75
# Subtracted when the alias does not start the span. English names are head-initial and the
# sampler draws spans as proper-noun runs, so the entity is what the span *starts* with and
# the tail is whatever else the headline capitalized. Measured: `Binance Helped Russia
# Target` matches `target` — Target Corp, an exact and complete company name — four tokens
# in, and without this a story about a crypto exchange linked to a retailer at confidence
# 0.90. It is a penalty rather than a veto because a leading word sometimes gets swept in
# (`Why Apple`), and a veto would turn those into silent misses instead of low-confidence
# ones the floor can rule on.
CONFIDENCE_BURIED_PENALTY = 0.30
# Subtracted when the alias is a single token. A two-word match is a more specific claim
# about the same span than a one-word match, and in this corpus the difference is most of
# the precision: `Getty Images` genuinely names Getty Images Holdings, while `carver`,
# `relay` and `trump` each start a real company's name and name none of them here.
CONFIDENCE_SINGLE_TOKEN_PENALTY = 0.10
# A single-token alias, where the token is also an everyday English word, and the context
# names the entity in full elsewhere — `Meta` where `Meta Platforms` appears nearby. High,
# because the precondition is strong: a multi-token complete name of the same entity, in the
# same context, is close to a direct match on the span itself.
#
# **It fires zero times across the 300 labeled mentions.** So this value is stated intent and
# nothing more — the labeled set neither supports nor contradicts it, and no published number
# below rests on it. It is kept because it is the only path by which the common-word class
# (`Meta`, `Apple`, `Block`) can ever link without embeddings, and deleting it would leave
# that class with none. Treat it as unmeasured until a corpus exercises it.
CONFIDENCE_CORROBORATED = 0.85
# The same, with nothing corroborating. Deliberately under any plausible floor: this is the
# `Amazon`-the-river case, and the value states that the word alone is not evidence.
CONFIDENCE_BARE_COMMON_WORD = 0.20

# A one or two character alias is a stock symbol or an initial, not a name a person wrote.
# `Li David H` would otherwise resolve through any two-letter entity in the dictionary.
MIN_ALIAS_CHARS = 3

# How far a span is allowed to grow into the surrounding capitalized run — see `expand`.
MAX_EXPANSION_TOKENS = 8

# `BofA Finance LLC (0001682472)`. EDGAR states the filer's CIK immediately after its name,
# which is what makes this channel safe: the CIK is attached to *this* span, not merely
# present in the same context. `Filed` and `AccNo` share that context and must not inherit it.
_TRAILING_CIK = re.compile(r"\s*\((\d{6,10})\)")

# A capitalized or numeric token — the units a name is built from. `88` counts, because
# `PIER 88 INVESTMENT PARTNERS LLC` is one name and dropping the number splits it in two.
_NAME_TOKEN = re.compile(r"^(?:[A-Z0-9][\w&.'-]*|(?:of|for|and|de|van|der|the))$")

UNLINKED_NOT_A_COMPANY = "not-a-company"
UNLINKED_AMBIGUOUS = "ambiguous"
UNLINKED_NO_SUCH_ENTITY = "no-such-entity"
UNLINKED_BELOW_FLOOR = "below-floor"


@dataclass(frozen=True)
class Resolution:
    """One decision, with its evidence. Mirrors SPEC §9's `entity_mentions` columns
    `entity_id`, `confidence` and `resolution_method`, so a row in that table is a record of
    why the link was made rather than only that it was."""

    entity_id: str | None
    confidence: float
    method: str
    reason: str | None = None
    # What actually matched, after expansion — `PIER` resolves through `pier 88 investment
    # partners`, and a run where that is invisible is a run nobody can debug.
    matched_alias: str | None = None

    @property
    def linked(self) -> bool:
        return self.entity_id is not None


def _unlinked(reason: str, confidence: float = 0.0, alias: str | None = None) -> Resolution:
    return Resolution(None, confidence, "unlinked", reason, alias)


def expand(surface_form: str, context: str) -> str:
    """Grow a span to the whole name it sits inside.

    The sampler's proper-noun heuristic cuts names in two: `PIER 88 INVESTMENT PARTNERS LLC`
    arrives as `PIER` and again as `INVESTMENT PARTNERS LLC`, and both are labeled with the
    same entity because a reader sees the whole name. So does this — expansion runs left and
    right across capitalized and numeric tokens and stops at the first thing that is neither,
    which in practice is a lowercase word, a bracket, or EDGAR's `-` separator.

    Over-expanding is safe on the lookup path, since `resolve` scans n-grams *inside* the
    result. It matters for minting, which is why minting additionally requires the expanded
    name to end in a legal form.
    """
    cleaned = strip_boilerplate(context or "")
    if not surface_form:
        return surface_form
    position = cleaned.find(surface_form)
    if position < 0:
        return surface_form

    before = cleaned[:position].split()
    after = cleaned[position + len(surface_form) :].split()
    left: list[str] = []
    for token in reversed(before[-MAX_EXPANSION_TOKENS:]):
        if not _NAME_TOKEN.match(token):
            break
        left.insert(0, token)
    right: list[str] = []
    for token in after[:MAX_EXPANSION_TOKENS]:
        if not _NAME_TOKEN.match(token):
            break
        right.append(token)
    # A leading connective (`of`, `the`) is only part of a name when something capitalized
    # precedes it; on its own at the edge it is the tail of the previous sentence.
    while left and not left[0][:1].isupper():
        left.pop(0)
    while right and not right[-1][:1].isupper():
        right.pop()
    return " ".join([*left, surface_form, *right])


def _cik_from_context(surface_form: str, context: str) -> str | None:
    """The CIK stated immediately after this span, if there is one."""
    cleaned = strip_boilerplate(context or "")
    position = cleaned.find(surface_form)
    if position < 0:
        return None
    match = _TRAILING_CIK.match(cleaned[position + len(surface_form) :])
    return match.group(1).zfill(10) if match else None


def _ngrams(tokens: tuple[str, ...]) -> list[tuple[int, tuple[str, ...]]]:
    """Every contiguous run, longest first, with where it starts.

    The offset is evidence, not bookkeeping — see `CONFIDENCE_BURIED_PENALTY`.
    """
    return [
        (start, tokens[start : start + length])
        for length in range(len(tokens), 0, -1)
        for start in range(len(tokens) - length + 1)
    ]


def _prominence(dictionary: Dictionary, entity_id: str) -> int:
    """Lower is more prominent. SEC's file order; anything without one sorts last."""
    entity = dictionary.entities.get(entity_id)
    rank = entity.rank if entity else None
    return rank if rank is not None else 1_000_000


def _candidate(dictionary: Dictionary, tokens: tuple[str, ...]) -> tuple[str, str] | None:
    """`(entity_id, method)` for one n-gram, or None if it matches nothing usable.

    A **complete** name match beats any number of prefix matches, which is what keeps `apple`
    on `Apple Inc.` rather than making it ambiguous with `Apple Hospitality REIT, Inc.`. Two
    complete matches are genuine ambiguity and resolve to nothing — there is no evidence here
    to separate two companies with the same registered name.

    Several *prefix* matches are a weaker kind of collision, and one that abstaining on would
    cost most of the well-known names in the corpus: `meta` starts `Meta Platforms`,
    `Metagenomi` and `Metallus`, and only one of those is ever the subject of a news story.
    So it resolves to the most prominent by SEC's own ordering, and says `prefix` — a method
    whose confidence is low enough that the fitted floor gets to decide whether prominence is
    evidence enough.
    """
    alias = dictionary.lookup(tokens)
    if alias is None:
        return None
    if len(alias.completes) == 1:
        return alias.completes[0], "name"
    if alias.completes:
        return None
    if alias.starts:
        return min(alias.starts, key=lambda e: (_prominence(dictionary, e), e)), "prefix"
    return None


def _corroborated(dictionary: Dictionary, context: str, entity_id: str) -> bool:
    """Whether the context names this entity somewhere in full.

    The lexical stand-in for SPEC §7.2's embedding similarity: `Meta` links when `Meta
    Platforms` is nearby, and does not when the sentence is about a river. It requires a
    multi-token match, since a second bare `Meta` corroborates nothing.
    """
    tokens = dict_module.normalize(context or "")
    for _, gram in _ngrams(tokens):
        if len(gram) < 2:
            break
        alias = dictionary.lookup(gram)
        if alias and entity_id in alias.completes:
            return True
    return False


def _is_noise(tokens: tuple[str, ...]) -> bool:
    """Whether a span is the feeds talking about themselves — `Filed`, `AccNo`, `Filer`."""
    return bool(tokens) and all(token in FEED_BOILERPLATE for token in tokens)


def resolve(
    surface_form: str,
    context: str = "",
    *,
    dictionary: Dictionary | None = None,
) -> Resolution:
    """Resolve one mention. The single decision point for SPEC §7.2.

    `context` is the surrounding text — the labeled set carries ±200 characters and the Spark
    job passes the same window. It is used for three things: the CIK a filing states, the
    full name a truncated span sits inside, and the corroboration a common word needs.
    """
    dictionary = dictionary if dictionary is not None else dict_module.load()

    raw_tokens = dict_module.normalize(surface_form)
    if not raw_tokens or _is_noise(raw_tokens):
        return _unlinked(UNLINKED_NOT_A_COMPANY)

    expanded = expand(surface_form, context)
    tokens = dict_module.normalize(expanded)

    # Every channel proposes, and the strongest evidence wins. Taking the first match instead
    # was measurably wrong: the sampler's spans are headline fragments, so `Binance Helped
    # Russia Target` contains `target` — Target Corp, an exact company name — and a
    # first-match rule linked a story about a crypto exchange to a retailer. Scanning
    # longest-n-gram-first does not fix it either, because the junk match is often the only
    # match. Weighing them does.
    candidates: list[Resolution] = []

    # 1. The identifier, if the source supplied one for this span. A CIK with no ticker is a
    # real registrant that is not tradable — a fund, a financing subsidiary, an individual —
    # so a miss here proposes nothing and lets the name channels answer.
    cik = _cik_from_context(surface_form, context) or _cik_from_context(expanded, context)
    if cik:
        entity_id = dictionary.by_cik.get(cik)
        if entity_id:
            candidates.append(Resolution(entity_id, CONFIDENCE_CIK, "cik", None, cik))
        elif not dict_module.has_legal_suffix(tokens):
            # The same channel, read as negative evidence, and it is the single most
            # valuable rule here. A CIK that no company holds belongs to a registrant that
            # is not a company — overwhelmingly an individual, since EDGAR Form 4 and 144
            # filers are officers and directors filing under their own names, surname first.
            # Those names start company names constantly: `Matthews Mark E.` linked to
            # Matthews International, `Greene Michelle D.` to Greene County Bancorp, `GEE
            # DAVID NICHOLAS` to GEE Group. Guessing from the surname alone is exactly what
            # a source-supplied identifier saves us from.
            #
            # A legal form overrides it, because a private fund also holds a CIK no ticker
            # claims — `PIER 88 INVESTMENT PARTNERS LLC` is a company EDGAR knows and the
            # ticker file does not.
            return _unlinked(UNLINKED_NOT_A_COMPANY)

    # 2 and 3. Every n-gram that names something.
    for start, gram in _ngrams(tokens):
        if sum(len(token) for token in gram) < MIN_ALIAS_CHARS:
            continue
        candidate = _candidate(dictionary, gram)
        if candidate is None:
            continue
        entity_id, method = candidate
        alias = " ".join(gram)
        rank = dictionary.word_rank(gram[0]) if len(gram) == 1 else None
        if rank is not None and rank < COMMON_WORD_RANK:
            confidence = (
                CONFIDENCE_CORROBORATED
                if _corroborated(dictionary, context, entity_id)
                else CONFIDENCE_BARE_COMMON_WORD
            )
        else:
            confidence = CONFIDENCE_NAME if method == "name" else CONFIDENCE_PREFIX
            if start > 0:
                confidence -= CONFIDENCE_BURIED_PENALTY
            if len(gram) == 1:
                confidence -= CONFIDENCE_SINGLE_TOKEN_PENALTY
        candidates.append(Resolution(entity_id, confidence, method, None, alias))

    # 4. A legal form and nothing to match it against: a company nobody has indexed. It
    # competes with the others rather than being a fallback — `BofA Finance LLC` proposes
    # both `bofa-finance` from its legal form and `finance` from a bare common word, and the
    # legal form is the better evidence by a distance.
    if dict_module.has_legal_suffix(tokens):
        minted = dict_module.slug(expanded)
        # **A minted id needs more than one word.** `has_legal_suffix` places the legal form
        # near the end; it cannot tell whether what precedes it is a name. `Investment Company
        # Act Section` puts `company` two from the end exactly as EDGAR's `... LLC SERIES A29`
        # shape does, and mints `investment` — a statute read as a filer. Requiring two tokens
        # separates them, and it is the same claim `CONFIDENCE_SINGLE_TOKEN_PENALTY` already
        # makes about matched aliases: one word is a weaker claim about a span than two.
        # Nothing is lost by it — an unindexed company whose whole name is one word plus a
        # legal form is reachable through the CIK its filing states.
        if minted and "-" in minted:
            candidates.append(Resolution(minted, CONFIDENCE_MINTED, "minted", None, minted))

    if not candidates:
        # A capitalized string that names no known entity and claims no legal form. In this
        # corpus that is overwhelmingly a person, a product or a headline fragment.
        return _unlinked(UNLINKED_NO_SUCH_ENTITY)

    # Confidence first; then the longer alias, because a longer name is a more specific claim
    # about the same span; then SEC prominence, so the order is total and a replay reproduces
    # it exactly.
    best = max(
        candidates,
        key=lambda r: (
            r.confidence,
            len(r.matched_alias or ""),
            -_prominence(dictionary, r.entity_id or ""),
        ),
    )
    return _finish(best)


def _finish(resolution: Resolution) -> Resolution:
    """Apply the confidence floor. SPEC §7.2's "unlinked rather than guessed", in one place
    so no channel can quietly opt out of it."""
    if resolution.confidence >= CONFIDENCE_FLOOR:
        return resolution
    return _unlinked(UNLINKED_BELOW_FLOOR, resolution.confidence, resolution.matched_alias)


def resolve_mention(
    surface_form: str, context: str = "", *, dictionary_path: Path | None = None
) -> Resolution:
    """`resolve`, for callers holding a path rather than a loaded dictionary — the eval
    harness and the tests. The dictionary is cached, so this is not a re-read per call."""
    return resolve(surface_form, context, dictionary=dict_module.load(dictionary_path))
