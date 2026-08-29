# ADR-0016 — The embedding model, pinned; and why it is a second model rather than the first

**Status:** Accepted · **Date:** 2026-08-29 · **Implements ADR-0009 §1 · Extends ADR-0003's pinning rule**

## Context

ADR-0009 adopted embeddings for SPEC §7.1 stage 3 and §7.4's novelty component, and chose the
vehicle on packaging grounds rather than accuracy: `sentence-transformers` costs 1.1 GB
installed, 722 MB of it torch, in a repo whose entire bronze path exists because of a 250 MB
Lambda ceiling. Ollama costs nothing new — ADR-0002 already runs it natively on the host and
4B already depends on it.

What ADR-0009 did not say is **which** model, and there was a real question underneath that:
`llama3.1:8b` is already installed, already pinned by ADR-0003, and Ollama will happily return
embeddings from it. Using it would have added no new dependency at all.

## Decision

**Pull `nomic-embed-text` and pin it by digest, as a second model alongside the generative one.**

```
ollama_embed_model        = "nomic-embed-text"
ollama_embed_model_digest = "sha256:970aa74c0a90ef7482477cf803618e776e173c007bf957f635f1015bfcfef0e6"
```

Read from `/api/show`'s modelfile `FROM` line, which is the model blob — not `/api/tags`'s
manifest digest, for the reason `enrich/client.py::local_model_digest` already documents at
length.

### Why not embed with `llama3.1:8b`

Three reasons, in order of how much they mattered:

1. **A decoder's hidden states are not a sentence embedding.** Llama is not trained with a
   contrastive objective; its pooled activations carry the next-token prediction signal, not a
   metric space where cosine means "about the same thing". `nomic-embed-text` is trained for
   exactly the retrieval objective this component needs.
2. **Size, in the wrong direction.** 4.9 GB versus 274 MB, for a job that runs over 8,691
   headlines per brief. The 137M-parameter model is not the compromise here; it is the correct
   tool, and it happens to be 18× smaller.
3. **The pins would collide.** ADR-0003's digest is the cache key for enrichment. If one model
   served both stages, swapping the generative model — an explicitly anticipated event, since
   §7.3 calls that "a measurement, not a vibe" — would silently invalidate every embedding as
   well, and the two caches would have to be reasoned about together forever.

### Why the digest is pinned at all

The same argument ADR-0003 makes, arriving at a sharper consequence. The digest is *part of the
cache key* (`enrich/embed.py::text_key`). An unpinned tag that Ollama re-pulls would leave a
cache full of vectors from one encoder being compared, by cosine, against vectors from another
— two coordinate systems in one dot product, producing plausible numbers that mean nothing. A
wrong embedding does not raise; it ranks.

## Measurement (dev box, RTX 5070 8 GB, 2026-08-29)

- **Model:** `nomic-embed-text`, `nomic-bert`, 137M params, F16, 274 MB, 768 dimensions,
  2048-token context.
- **Separation**, the property the component depends on:

  | pair | cosine |
  |---|---|
  | "Northwind acquires Lumen Robotics" / "Northwind to buy Lumen Robotics" | **0.9223** |
  | "Northwind acquires Lumen Robotics" / "A recipe for sourdough bread" | **0.3661** |

- **Throughput: 148 embeddings/sec at batch 64.** The full working set — 7,011 history heads
  plus 1,680 current — is under a minute cold and near zero warm.

### The batch size is a stability number, and this is worth recording

A synthetic benchmark said 256 was fastest: ~247/sec against ~10/sec at 32, which is
model-load amortisation rather than a throughput curve. Against 7,011 *real* titles the second
batch of 256 returned:

```
{"error": "Post \"http://127.0.0.1:59814/tokenize\": dial tcp 127.0.0.1:59814:
           connectex: No connection could be made because the target machine actively refused it."}
```

That is Ollama's **runner subprocess**, not the request — the API surfacing its own dead
socket. Every row in the failing batch embedded fine on its own, so it was not a bad input;
the runner had died between batches. 64 completed 2,048 titles with zero failed batches at
148/sec, 128 managed 112/sec. The throughput cost is real and it buys a run that finishes.

A retry with backoff sits on top, because the runner can die at any batch size and a single
death should not be fatal to a 47-second run.

**The synthetic benchmark was wrong in the way benchmarks usually are**: identical-length
generated strings, no memory pressure, and a conclusion that inverted on contact with the
corpus. Recorded because the number it produced was the one a reasonable person would have
shipped.

## Consequences

- A second model on the host, and a second thing to install. `make setup` does not pull it;
  `enrich/embed.py` raises `OllamaUnavailable` and the brief degrades to novelty 0 with a
  printed warning, exactly as it does when Ollama is off entirely.
- The embedding cache is a local Parquet file under `data/`, not an Iceberg table. SPEC §14
  argues the working set is "a numpy array and a cosine call, not a database", ADR-0015
  measured it at 11,267 vectors against a 50,000 gate, and losing the cache costs ~40 seconds.
- `gold.cluster_enrichment` and the embedding cache now key on **different digests**, which is
  the point of item 3 above: either model can be swapped without invalidating the other's work.
- Novelty degrades to 0 rather than blocking a render. A brief that refuses to build because
  the GPU is asleep would be a worse product than one whose novelty column reads zero — and
  the reader can tell which, because `score_components` records it.
