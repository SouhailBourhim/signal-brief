"""Content hashing, simhash, and the LLM cache key. SPEC §7.1, §7.3."""

from __future__ import annotations

import hashlib
import re

_WS = re.compile(r"\s+")


def normalize_for_hash(text: str) -> str:
    """Collapse whitespace and case so trivial formatting differences hash alike.

    Deliberately conservative: this is the *exact* duplicate stage (§7.1 step 1), and
    over-normalizing here steals work from the simhash stage, which can express degrees
    of similarity that a hash cannot.
    """
    return _WS.sub(" ", text.strip()).lower()


def content_hash(payload: bytes | str) -> str:
    if isinstance(payload, str):
        payload = normalize_for_hash(payload).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _shingles(text: str, k: int = 1) -> list[str]:
    words = normalize_for_hash(text).split()
    if len(words) < k:
        return [" ".join(words)] if words else []
    return [" ".join(words[i : i + k]) for i in range(len(words) - k + 1)]


def simhash64(text: str, k: int = 1) -> int:
    """64-bit simhash over word shingles. SPEC §7.1 stage 2.

    Catches near-identical text: reprints with headline tweaks and light edits. It does
    NOT catch "Apple acquires X" vs "X to be bought by Apple" — that is stage 3's job,
    and expecting it here is how a dedup layer quietly under-performs.

    `k=1` (unigrams) is measured, not assumed. On article-length news text a one-word
    edit lands at ~3 bits with unigrams versus ~17 with 4-word shingles, because a
    20-token article yields too few long shingles for the bit vector to be stable. Phase 3
    re-tunes this against `evals/dedup` with real articles rather than a fixture.
    """
    vector = [0] * 64
    grams = _shingles(text, k)
    if not grams:
        return 0
    for gram in grams:
        h = int.from_bytes(hashlib.blake2b(gram.encode("utf-8"), digest_size=8).digest(), "big")
        for bit in range(64):
            vector[bit] += 1 if (h >> bit) & 1 else -1
    out = 0
    for bit in range(64):
        if vector[bit] > 0:
            out |= 1 << bit
    return out


def hamming(a: int, b: int) -> int:
    return ((a ^ b) & ((1 << 64) - 1)).bit_count()


def enrichment_cache_key(input_text: str, model_digest: str, prompt_version: str) -> str:
    """SPEC §7.3: cache keyed on (input_hash, model_digest, prompt_version).

    All three participate, so changing the prompt or swapping the model invalidates the
    cache rather than silently serving output the current configuration would not have
    produced. That property is what lets §11 report cache-hit rate as a real metric.
    """
    payload = "\x1f".join([content_hash(input_text), model_digest, prompt_version])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
