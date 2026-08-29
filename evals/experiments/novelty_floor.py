#!/usr/bin/env python3
"""What "recycled" actually looks like in this corpus. SPEC §7.4; ADR-0009, ADR-0016; 5.C.

SPEC §7.4 defines novelty as "embedding distance to the last 30 days of clusters — recycled
narratives sink". That is a definition, not a threshold. `1 - cosine` is not usable as a score
directly, because sentence embeddings of *any* two English headlines sit well above zero: the
smoke test that opened 5.C put two phrasings of one story at 0.92 and an unrelated recipe at
0.37, so the usable range is the top fifth of the scale and a raw `1 - cos` would hand every
story most of the weight.

So the component needs a floor — the similarity below which a story is simply not about
anything recent — and this measures where to put it rather than guessing. Same discipline as
`corpus_merge_rate.py`: the constant comes out of the corpus, and the script that produced it
is committed next to it.

    uv run python evals/experiments/novelty_floor.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from signal_core.brief.read import read_novelty_history, read_window_heads
from signal_core.config import Settings
from signal_core.enrich.embed import EmbeddingCache, max_similarity


def main() -> int:
    settings = Settings()
    current, current_query = read_window_heads()
    history, history_query = read_novelty_history()
    print(f"current window : {len(current):>6} heads")
    print(f"prior 30 days  : {len(history):>6} heads")
    print(
        f"scanned        : {current_query.bytes_scanned + history_query.bytes_scanned:,} bytes, "
        f"${current_query.cost_usd + history_query.cost_usd:.6f}"
    )

    cache = EmbeddingCache(settings.embedding_cache_path, settings.ollama_embed_model_digest)
    print(f"cache          : {cache.load()} vectors")

    history_vectors = cache.embed([h["title"] for h in history], settings, progress=print)
    current_vectors = cache.embed([c["title"] for c in current], settings, progress=print)
    cache.save()

    scores = sorted(max_similarity(v, history_vectors) for v in current_vectors)
    n = len(scores)
    print("\nmax similarity of each current head to the prior 30 days:")
    for label, index in (
        ("min ", 0),
        ("p10 ", n // 10),
        ("p25 ", n // 4),
        ("p50 ", n // 2),
        ("p75 ", 3 * n // 4),
        ("p90 ", 9 * n // 10),
        ("max ", n - 1),
    ):
        print(f"  {label} {scores[index]:.4f}")

    print("\nshare of the window at or above each candidate floor:")
    for floor in (0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.99):
        above = sum(1 for s in scores if s >= floor)
        print(f"  >= {floor:.2f}   {above:>6} / {n}  = {above / n:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
