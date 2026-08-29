"""Embeddings, for SPEC §7.4's novelty component. ADR-0009, ADR-0016.

## Why this is here and not in a vector database

ADR-0009 adopted embeddings and chose the vehicle on packaging grounds: `sentence-transformers`
costs 1.1 GB installed, 722 MB of it torch, in a repo whose entire bronze path exists because
of a 250 MB ceiling. Ollama costs nothing new — ADR-0002 already puts it on the host, 4B
already depends on it, and reaching it is an httpx call.

SPEC §14 settles the storage question in the same spirit, and ADR-0015 measured it: the working
set is **11,267 vectors** against a 50,000 gate. So this is a dict of lists and a dot product,
not Postgres. The cache is a local Parquet file, which is the honest shape for something whose
loss costs forty seconds of recomputation.

## The cache key is the text, not the cluster

Same argument as `enrich/run.py::cluster_input`: keying on `cluster_id` would re-embed the same
headline three times, because a rolling 72-hour window re-clusters every story on three
consecutive days under three different ids. Keying on the text means those three share one
vector — and it means a re-run over an unchanged corpus makes no inference calls at all.

The model digest is part of the key. A vector from a different encoder is not a cheaper answer
to the same question, it is an answer to a different one, and mixing them silently would put
two coordinate systems in one cosine.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import httpx

from signal_core.config import Settings
from signal_core.enrich.client import OllamaUnavailable

# **64, and it is a stability number rather than a throughput one.** A synthetic benchmark
# said 256 was fastest (~247/sec against ~10/sec at 32, which is model-load amortisation, not
# a curve). Against 7,011 real titles the second batch of 256 returned:
#
#     {"error": "Post \"http://127.0.0.1:59814/tokenize\": dial tcp ... actively refused"}
#
# — Ollama's *runner subprocess*, not the request. Every row in that batch embedded fine on its
# own, so it was not a bad input; the runner had died between batches and the API surfaced its
# own dead socket. 64 completed 2,048 titles with zero failed batches at 148/sec, and 128 at
# 112/sec. The throughput cost is real and it buys a run that finishes.
BATCH = 64

# The runner can die at any batch size, so the retry is not a substitute for the batch size —
# it is what makes a single death survivable rather than fatal to a 47-second run.
RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0

EMBED_TIMEOUT = 300.0


def text_key(text: str, model_digest: str) -> str:
    """The cache key for one text under one encoder. See the module docstring."""
    digest = hashlib.sha256()
    digest.update(model_digest.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(" ".join(text.split()).encode("utf-8"))
    return digest.hexdigest()


def embed_texts(
    texts: Sequence[str],
    settings: Settings | None = None,
    *,
    timeout: float = EMBED_TIMEOUT,
    progress: Any | None = None,
) -> list[list[float]]:
    """Embed `texts` in order, batching. Raises `OllamaUnavailable` if the host is not there."""
    settings = settings or Settings()
    if not texts:
        return []

    vectors: list[list[float]] = []
    base = settings.ollama_url.rstrip("/")
    try:
        with httpx.Client(base_url=base, timeout=timeout) as client:
            for start in range(0, len(texts), BATCH):
                chunk = list(texts[start : start + BATCH])
                vectors.extend(_embed_batch(client, chunk, settings))
                if progress:
                    progress(f"        embedded {len(vectors)}/{len(texts)}")
    except httpx.HTTPError as exc:
        raise OllamaUnavailable(f"{base}: {exc}") from exc
    except ValueError as exc:
        raise OllamaUnavailable(f"{base}: non-JSON response") from exc
    return vectors


def _embed_batch(client: httpx.Client, chunk: list[str], settings: Settings) -> list[list[float]]:
    """One batch, retried. See `RETRIES` for what is being retried and why."""
    last: Exception | None = None
    for attempt in range(RETRIES):
        try:
            response = client.post(
                "/api/embed",
                json={"model": settings.ollama_embed_model, "input": chunk},
            )
            response.raise_for_status()
            batch = response.json().get("embeddings") or []
            if len(batch) != len(chunk):
                raise OllamaUnavailable(f"asked for {len(chunk)} embeddings, got {len(batch)}")
            return list(batch)
        except (httpx.HTTPError, ValueError) as exc:
            last = exc
            if attempt < RETRIES - 1:
                # The runner reloads on the next request; the wait is for it to finish dying.
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise OllamaUnavailable(f"batch of {len(chunk)} failed after {RETRIES} attempts: {last}")


class EmbeddingCache:
    """Content-hash keyed vectors, persisted as Parquet.

    Deliberately not an Iceberg table. `gold.cluster_enrichment` is one because an enrichment
    is an expensive, non-reproducible model output that the brief cites and lineage has to
    account for. A vector is none of those: it is reproducible from the text and the pinned
    digest, it is never shown to anyone, and 11k of them round-trip through Athena far more
    slowly than they recompute.
    """

    def __init__(self, path: Path, model_digest: str) -> None:
        self.path = path
        self.model_digest = model_digest
        self._vectors: dict[str, list[float]] = {}

    def load(self) -> int:
        if not self.path.exists():
            return 0
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        keys = table.column("key").to_pylist()
        vectors = table.column("vector").to_pylist()
        self._vectors = dict(zip(keys, vectors, strict=True))
        return len(self._vectors)

    def save(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "key": list(self._vectors),
                "vector": [self._vectors[k] for k in self._vectors],
            }
        )
        pq.write_table(table, self.path)

    def embed(
        self,
        texts: Sequence[str],
        settings: Settings | None = None,
        *,
        progress: Any | None = None,
    ) -> list[list[float]]:
        """Vectors for `texts`, calling the model only for what is not already held."""
        keys = [text_key(t, self.model_digest) for t in texts]
        missing = [t for t, k in zip(texts, keys, strict=True) if k not in self._vectors]
        # De-duplicated before the call, not after: the same headline appears in three
        # consecutive windows, and paying for it three times is the thing the cache is for.
        unique = list(dict.fromkeys(missing))
        if unique:
            if progress:
                progress(f"        {len(unique)} new to embed, {len(self._vectors)} cached")
            fresh = embed_texts(unique, settings, progress=progress)
            for text, vector in zip(unique, fresh, strict=True):
                self._vectors[text_key(text, self.model_digest)] = normalize(vector)
        return [self._vectors[k] for k in keys]


def normalize(vector: Sequence[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vector))
    return [x / norm for x in vector] if norm else list(vector)


def max_similarity(vector: Sequence[float], corpus: Iterable[Sequence[float]]) -> float:
    """Cosine similarity to the nearest of `corpus`, or 0.0 if it is empty.

    Both sides are pre-normalized by `EmbeddingCache.embed`, so this is a dot product.
    Written out rather than reached for numpy: the caller already holds plain lists, and 11k
    dot products of 768 floats is milliseconds.
    """
    best = 0.0
    for other in corpus:
        total = 0.0
        for a, b in zip(vector, other, strict=False):
            total += a * b
        best = max(best, total)
    return best
