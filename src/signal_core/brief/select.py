"""Which clusters, in what order — the read-and-rank both consumers share.

Two stages now need the same ranked window: `brief/build.py` renders the top of it, and 4B's
`enrich/run.py` runs the model over a larger slice of it before the brief is built. They must
agree, because enrichment that ranked differently from the brief would spend its budget on
stories the brief does not show and leave the ones it does show unenriched.

Sharing the function is what makes them agree. Two copies of the read sequence would drift
the first time a component was added to `WEIGHTS` and only one copy was updated — the same
argument `build.py::_optional_read` makes about its own generalization, one level up.

**Ranking reads nothing that enrichment writes**, so there is no circularity in ordering it
this way: §7.4's `WEIGHTS` has no enrichment component, and 4B did not add one.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from signal_core.brief.ranker import rank
from signal_core.brief.read import (
    CLUSTER_WINDOW_HOURS,
    ClusterRead,
    read_cluster_entities,
    read_clusters,
    read_feedback,
    read_hn_velocity,
    read_market_moves,
    read_novelty_history,
)
from signal_core.config import Settings
from signal_core.ops.athena import AthenaQueryFailed, QueryResult
from signal_core.timeutil import utc_now
from signal_core.watchlist import load as load_watchlist

# How far back a mark still counts. Marks are keyed on `cluster_id`, and consecutive daily
# runs can assign a story a new one, so a longer window buys nothing — this is "have I
# already seen this", not a training history (SPEC §14).
FEEDBACK_LOOKBACK_DAYS = 7

# Athena's spelling of "you asked about a table that does not exist". Matched on the message
# because the API returns `FAILED` with a reason string rather than a typed error code.
_MISSING_TABLE = ("does not exist", "not found", "table_not_found")

EMPTY_QUERY = QueryResult(rows=[], bytes_scanned=0, engine_execution_ms=0, cost_usd=0.0)


def optional_read(
    read: Callable[[], tuple[Any, QueryResult]],
    *,
    warning: str,
    progress: Callable[[str], None],
) -> tuple[Any, QueryResult]:
    """Run a read that the caller can do without, degrading to nothing if its table is absent.

    Every one of these is **additive** — the stories are the product, and a fresh clone or a
    new account that has run `cluster` but not `resolve`, `market` or the 4A/4B DAGs should
    still get its morning read. So a missing table degrades to an empty result plus a loud
    line, rather than taking down the page.

    Narrow on purpose: only "no such table" is swallowed. A permissions error, a malformed
    query or a workgroup cutoff still raises, because those are faults in a table that is
    supposed to be there, and quietly rendering a diminished brief would hide them.

    Generalized from 3.D's `_read_entities`, which made this argument first; moved here in 4B
    when a second module needed it.
    """
    try:
        return read()
    except AthenaQueryFailed as failure:
        message = str(failure).lower()
        if not any(marker in message for marker in _MISSING_TABLE):
            raise
        progress(f"        WARNING: {warning}")
        return None, EMPTY_QUERY


@dataclass(frozen=True)
class RankedWindow:
    """One read-and-rank pass. Every field is something a caller prints or scores against."""

    clusters: list[dict[str, Any]]
    cluster_read: ClusterRead
    entities: dict[str, list[dict[str, Any]]]
    velocity_slopes: dict[str, float]
    market_moves: dict[str, float]
    feedback: dict[str, float]
    cluster_query: QueryResult
    entity_query: QueryResult
    velocity_query: QueryResult
    market_query: QueryResult
    feedback_query: QueryResult
    novelty_query: QueryResult

    @property
    def queries(self) -> tuple[QueryResult, ...]:
        return (
            self.cluster_query,
            self.entity_query,
            self.velocity_query,
            self.market_query,
            self.feedback_query,
            self.novelty_query,
        )

    @property
    def linked_entities(self) -> int:
        return sum(len(cluster.get("entities") or []) for cluster in self.clusters)


def ranked_window(
    *,
    limit: int = 10,
    window_hours: int = CLUSTER_WINDOW_HOURS,
    now: datetime | None = None,
    client: Any | None = None,
    progress: Callable[[str], None] = lambda _: None,
) -> RankedWindow:
    """Read the newest clustered window, attach every ranking input, score and cut.

    `progress` exists so `build.py` can keep printing its numbered steps as the reads happen
    rather than after all of them, while `enrich/run.py` stays quiet. Defaulting it to a
    no-op means a caller that does not care never has to think about it.
    """
    now = now or utc_now()
    since = now - timedelta(hours=window_hours)

    cluster_read, cluster_query = read_clusters(since, now, client=client)
    clusters = cluster_read.clusters

    entities, entity_query = optional_read(
        lambda: read_cluster_entities(since, now, client=client),
        warning=(
            "no entity tables yet — has the resolve DAG run? "
            "Stories are unaffected; company links are missing from this brief."
        ),
        progress=progress,
    )
    entities = entities or {}
    for cluster in clusters:
        cluster["entities"] = entities.get(cluster["cluster_id"], [])

    velocity_slopes, velocity_query = optional_read(
        lambda: read_hn_velocity(since, client=client),
        warning="no hn_score_snapshots yet — velocity scores 0 for every story (4A.B)",
        progress=progress,
    )
    market_moves, market_query = optional_read(
        lambda: read_market_moves(client=client),
        warning="no market_observations yet — market corroboration scores 0 (4A.D)",
        progress=progress,
    )
    feedback, feedback_query = optional_read(
        lambda: read_feedback(now - timedelta(days=FEEDBACK_LOOKBACK_DAYS), client=client),
        warning="no brief_items yet — no marks to carry forward (4A.I)",
        progress=progress,
    )
    velocity_slopes = velocity_slopes or {}
    market_moves = market_moves or {}
    feedback = feedback or {}

    novelty, novelty_query = _read_novelty(clusters, now, client=client, progress=progress)

    ranked = rank(
        clusters,
        limit=limit,
        now=now,
        watchlist=load_watchlist(),
        velocity_slopes=velocity_slopes,
        market_moves=market_moves,
        feedback=feedback,
        novelty=novelty,
    )

    return RankedWindow(
        clusters=ranked,
        cluster_read=cluster_read,
        entities=entities,
        velocity_slopes=velocity_slopes,
        market_moves=market_moves,
        feedback=feedback,
        cluster_query=cluster_query,
        entity_query=entity_query,
        velocity_query=velocity_query,
        market_query=market_query,
        feedback_query=feedback_query,
        novelty_query=novelty_query,
    )


def _read_novelty(
    clusters: list[dict[str, Any]],
    now: datetime,
    *,
    client: Any | None,
    progress: Callable[[str], None],
) -> tuple[dict[str, float], QueryResult]:
    """SPEC §7.4's novelty, or an empty map and a loud line if the encoder is not reachable.

    **Degrades like every other optional signal, and for a sharper reason.** The other reads
    fall back when a *table* is missing; this one also has to fall back when Ollama is not
    running, because ADR-0002 puts it on the host and the host is a laptop. A brief that
    refuses to render because the GPU is asleep would be a worse product than one whose
    novelty column reads zero — and the reader can see which, because the warning prints and
    `score_components` records the zeros.

    Only the current window is embedded against history, not every pair: 1,680 heads against
    7,011 is 11.8M dot products of 768 floats, which is seconds, and SPEC §14's pgvector row
    is the argument for why that is the right shape rather than a database.
    """
    from signal_core.brief.novelty import score_novelty
    from signal_core.enrich.client import OllamaUnavailable
    from signal_core.enrich.embed import EmbeddingCache

    settings = Settings()
    history, novelty_query = optional_read(
        lambda: read_novelty_history(now, client=client),
        warning="no story_clusters history yet — novelty scores 0 for every story (5.C)",
        progress=progress,
    )
    if not history:
        return {}, novelty_query

    heads = [
        {"cluster_id": c["cluster_id"], "title": c.get("title") or ""}
        for c in clusters
        if c.get("title")
    ]
    try:
        cache = EmbeddingCache(settings.embedding_cache_path, settings.ollama_embed_model_digest)
        cached = cache.load()
        history_vectors = cache.embed([h["title"] for h in history], settings)
        head_vectors = cache.embed([h["title"] for h in heads], settings)
        cache.save()
    except OllamaUnavailable as unavailable:
        progress(f"        WARNING: no embeddings — novelty scores 0 ({unavailable})")
        return {}, novelty_query

    scores = score_novelty(heads, head_vectors, history_vectors)
    recycled = sum(1 for v in scores.values() if v == 0.0)
    progress(
        f"        novelty over {len(history)} prior heads, {cached} vectors cached, "
        f"{recycled}/{len(scores)} fully recycled"
    )
    return scores, novelty_query
