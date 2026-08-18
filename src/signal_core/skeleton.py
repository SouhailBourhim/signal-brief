"""The Phase 0 walking skeleton: fake source -> bronze -> silver -> clusters -> brief.

No network, no AWS, no LLM. Its job is to prove the contract, the path convention, and
the render path hold together before any of those enter the picture — and to keep
proving it from CI on a machine that is not the author's.

Spark is used for normalize (and only normalize) because the point is to exercise the
real toolchain, not to simulate it.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from signal_core.brief.ranker import rank
from signal_core.brief.render import write_brief
from signal_core.config import SOURCES, Settings
from signal_core.contracts import State
from signal_core.dedup import exact_dedup, group_stories
from signal_core.ops.health import RunHealth, assess_source
from signal_core.sources import get_poller
from signal_core.storage import write_bronze
from signal_core.transform import normalize_document


def _normalize_with_spark(bronze_root: Path) -> list[dict[str, Any]]:
    from signal_core.spark.jobs.normalize import build_session, normalize, split_rejects

    spark = build_session("signal-skeleton")
    try:
        clean, rejects = split_rejects(normalize(spark, bronze_root))
        reject_count = rejects.count()
        if reject_count:
            print(f"  quarantined {reject_count} unparseable rows (SPEC 6.2)")
        return [row.asDict() for row in clean.collect()]
    finally:
        spark.stop()


def _normalize_in_process(documents) -> list[dict[str, Any]]:
    """Spark-free fallback so the skeleton still runs without a JVM.

    Identical logic — both paths call `normalize_document` — so this is a transport
    difference only. CI runs the Spark path; this exists so a contributor without a JVM
    is not blocked, and it prints loudly so nobody mistakes it for a green Spark run.
    """
    rows = [
        normalize_document(
            {
                "source_id": d.source_id,
                "fetched_at": d.fetched_at,
                "content_hash": d.content_hash,
                "payload": d.payload,
            }
        )
        for d in documents
    ]
    return [r for r in rows if not r["parse_error"]]


def run(settings: Settings | None = None, use_spark: bool = True, limit: int = 10) -> Path:
    started = time.monotonic()
    settings = settings or Settings()
    config = SOURCES["fake"]

    print("[1/5] poll   — fake source")
    documents, state = get_poller("fake")(config, State(source_id="fake"))
    print(f"        {len(documents)} raw documents")

    print("[2/5] bronze — immutable parquet")
    bronze_root = settings.bronze_root
    files = write_bronze(documents, bronze_root)
    print(f"        {len(files)} files under {bronze_root}")

    if use_spark:
        print("[3/5] silver — Spark normalize")
        articles = _normalize_with_spark(bronze_root)
    else:
        print("[3/5] silver — in-process normalize (NO SPARK — not a toolchain check)")
        articles = _normalize_in_process(documents)
    flagged = sum(1 for a in articles if a["timestamp_flagged"])
    print(f"        {len(articles)} articles, {flagged} with unverified timestamps")

    print("[4/5] cluster— exact dedup + near-duplicate grouping")
    deduped, exact_removed = exact_dedup(articles)
    clusters = group_stories(deduped)
    print(f"        {len(articles)} in -> {len(clusters)} clusters ({exact_removed} exact dupes)")

    print("[5/5] brief  — rank + render")
    ranked = rank(clusters, limit=limit)
    health = RunHealth(
        sources=[assess_source(config, len(documents), state.last_success_at)],
        articles_in=len(articles),
        clusters_out=len(clusters),
        exact_duplicates_removed=exact_removed,
        cache_hit_rate=0.0,
        runtime_seconds=time.monotonic() - started,
    )
    path = write_brief(ranked, health, settings.out_root)
    print(f"        {path}")
    return path
