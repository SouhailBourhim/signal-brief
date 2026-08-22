"""The enrichment stage. SPEC §7.3; docs/runbooks/phase-4b.md 4B.D.

Read the ranked head of the window, serve what the cache already holds, infer the rest,
validate every answer, quarantine what fails, and report a hit rate that means something.

## Why the *ranked* head rather than every cluster

The obvious reading of §7.3 — "enrichment runs against cluster heads" — would enrich every
cluster in the window. Measured against the deployed lake before this module was written,
**57% of clusters are SEC filings**, almost all of them routine fund and trust administration
(`ABS-EE`, `N-PX`, `NPORT-P`, `424B2`) with `article_count = 1` and no editorial content.
Enriching all of them would spend the majority of every inference budget on documents that
will never appear in a brief.

ADR-0003's capacity paragraph already assumed a bounded set — it sizes the measurement at "a
40-head batch", not a ten-thousand-head one — so `ENRICH_TOP_N` is that number, and the
selection is `brief/select.py::ranked_window`, the same read-and-rank the brief itself uses.
Enriching a set four times the size of the brief's cut is the margin that absorbs any
ranking drift between the pre-brief run and 07:00.

There is no circularity: §7.4's `WEIGHTS` has no enrichment component, so ranking does not
read what this writes.

## The retry bound

§7.3 says failures are "never retried indefinitely". A head that makes the model emit
unparseable output tends to keep doing so, and without a bound it would cost an inference
every morning forever. `store.MAX_ATTEMPTS` caps it; the count lives in
`gold.enrichment_rejects` so the bound survives a restart, and an exhausted cluster is
reported as skipped rather than silently passed over.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from signal_core.brief.read import CLUSTER_WINDOW_HOURS
from signal_core.brief.select import ranked_window
from signal_core.config import Settings
from signal_core.enrich import prompt as prompt_module
from signal_core.enrich import store
from signal_core.enrich.client import OllamaUnavailable, generate, supports_schema_format
from signal_core.enrich.schema import Enrichment
from signal_core.hashing import enrichment_cache_key
from signal_core.ops.athena import QueryResult
from signal_core.timeutil import utc_now

# ADR-0003's measured batch size. Four times the brief's default cut of 10, which is the
# margin against ranking drift between this run and the 07:00 send.
ENRICH_TOP_N = 40


def cluster_input(cluster: dict[str, Any]) -> str:
    """The exact text one cluster is enriched from.

    Hashing *this* rather than the cluster id is what gives the cache its useful properties:
    a re-run over unchanged text is a hit, an edited headline is a miss, and two clusters
    whose heads are identical share one inference.
    """
    return prompt_module.render(
        title=cluster.get("title") or "",
        publisher=cluster.get("publisher_domain") or "",
        body=cluster.get("snippet") or cluster.get("body_text") or "",
    )


@dataclass
class EnrichmentRun:
    """What one pass did. Every field is something a person would ask about afterwards."""

    processed: int = 0
    inferred: int = 0
    cache_hits: int = 0
    rejected: int = 0
    skipped_exhausted: int = 0
    written: int = 0
    elapsed_seconds: float = 0.0
    unavailable: str | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def cache_hit_rate(self) -> float:
        """SPEC §11's published metric: the share of this run's clusters served without
        calling the model. Zero processed is a rate of 0.0, not a division by zero — and the
        brief prints it beside a story count, so a reader can tell the two apart."""
        return self.cache_hits / self.processed if self.processed else 0.0


def _cache_keys(clusters: list[dict[str, Any]], settings: Settings) -> dict[str, str]:
    """`cluster_id` -> the three-part cache key its head text produces."""
    return {
        cluster["cluster_id"]: enrichment_cache_key(
            cluster_input(cluster), settings.ollama_model_digest, settings.prompt_version
        )
        for cluster in clusters
    }


def read_for_clusters(
    clusters: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
    client: Any | None = None,
) -> tuple[dict[str, store.CachedEnrichment], QueryResult]:
    """Enrichment for these clusters, keyed by `cluster_id`. What the brief calls.

    Looked up by the hash of the head text, not by cluster id, so **a story whose headline
    changed since it was enriched correctly comes back empty** rather than carrying a summary
    of the earlier version. Showing a stale summary under a new headline is the failure this
    lookup is shaped to prevent.
    """
    settings = settings or Settings()
    if not clusters:
        return {}, store.EMPTY_RESULT
    keys = _cache_keys(clusters, settings)
    cached, query = store.read_cached(
        sorted(set(keys.values())),
        model_digest=settings.ollama_model_digest,
        prompt_version=settings.prompt_version,
        client=client,
    )
    found = {cluster_id: cached[key] for cluster_id, key in keys.items() if key in cached}
    return found, query


def enrich_clusters(
    clusters: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
    client: Any | None = None,
    schema_format: bool | None = None,
) -> EnrichmentRun:
    """Enrich exactly these clusters. Returns what happened."""
    settings = settings or Settings()
    now = now or utc_now()
    started = time.monotonic()
    result = EnrichmentRun(processed=len(clusters))
    if not clusters:
        return result

    store.ensure_tables(client=client)

    keys = _cache_keys(clusters, settings)
    all_hashes = sorted(set(keys.values()))

    existing, _ = store.read_rows(
        all_hashes,
        model_digest=settings.ollama_model_digest,
        prompt_version=settings.prompt_version,
        client=client,
    )
    have_pair = {(row.cluster_id, row.input_hash) for row in existing}
    by_hash = {row.input_hash: row for row in existing}
    attempts = store.read_attempts(all_hashes, client=client)

    to_write: list[tuple[str, str, Enrichment, bool]] = []
    rejects: list[tuple[str, str, str, str, int]] = []
    # Populated as this run infers, so two clusters sharing head text inside one batch cost
    # one call rather than two — the same saving the stored cache gives across runs.
    inferred_this_run: dict[str, Enrichment] = {}

    if schema_format is None:
        schema_format = supports_schema_format(settings)
    json_schema = Enrichment.model_json_schema() if schema_format else None

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]
        key = keys[cluster_id]

        if (cluster_id, key) in have_pair:
            result.cache_hits += 1
            continue

        if key in by_hash or key in inferred_this_run:
            # Another cluster's identical head text. Copy rather than re-infer; this is the
            # one kind of hit a stored row can honestly record (see `store`'s docstring).
            enrichment = inferred_this_run.get(key)
            if enrichment is None:
                row = by_hash[key]
                try:
                    enrichment = Enrichment.model_validate(
                        {
                            "summary": row.summary,
                            "topic": row.topic,
                            "extraction": row.extraction,
                        }
                    )
                except ValidationError:
                    enrichment = None
            if enrichment is not None:
                to_write.append((cluster_id, key, enrichment, True))
                result.cache_hits += 1
                continue

        if attempts.get(key, 0) >= store.MAX_ATTEMPTS:
            result.skipped_exhausted += 1
            continue

        try:
            generation = generate(cluster_input(cluster), settings=settings, schema=json_schema)
        except OllamaUnavailable as exc:
            # Stop the batch rather than logging the same failure once per cluster. The
            # server being off is one fact, not forty, and forty quarantine rows about it
            # would bury the validation failures the table exists to surface.
            result.unavailable = str(exc)
            break

        result.inferred += 1
        try:
            enrichment = Enrichment.model_validate_json(generation.text)
        except ValidationError as exc:
            rejects.append((cluster_id, key, generation.text, str(exc), attempts.get(key, 0) + 1))
            result.rejected += 1
            continue

        inferred_this_run[key] = enrichment
        to_write.append((cluster_id, key, enrichment, False))

    result.written = store.write_enrichments(
        to_write,
        model_name=settings.ollama_model,
        model_digest=settings.ollama_model_digest,
        prompt_version=settings.prompt_version,
        now=now,
        client=client,
    )
    store.write_rejects(
        rejects,
        model_digest=settings.ollama_model_digest,
        prompt_version=settings.prompt_version,
        now=now,
        client=client,
    )
    result.elapsed_seconds = time.monotonic() - started
    return result


def run(
    *,
    limit: int = ENRICH_TOP_N,
    window_hours: int = CLUSTER_WINDOW_HOURS,
    settings: Settings | None = None,
    now: datetime | None = None,
    client: Any | None = None,
) -> EnrichmentRun:
    """Select the ranked head of the window and enrich it. What the enrich DAG calls."""
    settings = settings or Settings()
    now = now or utc_now()
    window = ranked_window(limit=limit, window_hours=window_hours, now=now, client=client)
    return enrich_clusters(window.clusters, settings=settings, now=now, client=client)
