"""Phase 4B DAG: the governed LLM stage. SPEC §7.3; ADR-0002, ADR-0003.

Runs the pinned local model over the ranked head of the window, writing
`gold.cluster_enrichment` and quarantining what fails validation to
`gold.enrichment_rejects`. The brief reads what this leaves behind; it never calls the model
itself, so a morning where Ollama was off produces a brief without summaries rather than a
brief that is late.

**06:15, between `cluster` at 05:00 and the brief at 07:00.** §7.3 is explicit that
enrichment runs "against cluster heads once per pre-brief window, not on every 15-minute
cycle", so this is a cron rather than an asset trigger on `CLUSTERS_COMMITTED` — a
15-minute-triggered enrichment would spend the GPU budget re-answering the same questions
about the same window all day. `CLUSTERS_COMMITTED` and `MENTIONS_RESOLVED` are declared as
inlets for graph visibility, the way `brief_dag` already declares its own.

**Ollama runs on the host, not in Compose** (ADR-0002: the GPU is why inference is free).
Compose reaches it at `host.docker.internal:11434` via `SIGNAL_OLLAMA_URL`, which
`docker-compose.yml` already sets. A task here failing with `OllamaUnavailable` most often
means the host process is not running.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import CLUSTERS_COMMITTED, ENRICHMENT_READY, MENTIONS_RESOLVED

# ADR-0003 measured ~22.5 s of model load plus ~1.0 s per head on the dev box (RTX 5070 8GB,
# llama3.1:8b q4). A 40-head batch is therefore ~1 minute. This bound is that figure with
# generous headroom for a cold GPU, a longer body, or a slower box — it is a tripwire for
# "enrichment has quietly become slow enough to threaten the 07:00 send", not a benchmark.
# SPEC §7.3: "The DAG asserts this bound and fails loudly rather than silently lagging."
CAPACITY_SECONDS_PER_HEAD = 8.0
CAPACITY_FLOOR_SECONDS = 60.0


@dag(
    dag_id="enrich",
    schedule="15 6 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,  # two batches against one GPU would contend for VRAM
    tags=["phase4b", "enrich"],
)
def enrich_dag():
    @task(inlets=[CLUSTERS_COMMITTED, MENTIONS_RESOLVED], outlets=[ENRICHMENT_READY])
    def enrich() -> dict[str, float | int | str | None]:
        from signal_core.enrich.run import ENRICH_TOP_N, run

        # `print` goes to the task log, which is the only place anyone watches this from.
        result = run(limit=ENRICH_TOP_N, progress=print)
        print(
            f"{result.processed} heads: {result.inferred} inferred, "
            f"{result.cache_hits} from cache ({result.cache_hit_rate:.0%}), "
            f"{result.rejected} quarantined, {result.skipped_exhausted} past the retry bound, "
            f"{result.written} rows written in {result.elapsed_seconds:.1f}s"
        )

        if result.unavailable:
            # Loud. The brief degrades gracefully without summaries, but a GPU stage that
            # silently stops working is the drift SPEC §11 exists to catch — and unlike a
            # documented limitation, this one is fixable by starting a process.
            raise RuntimeError(f"Ollama unavailable, batch stopped early: {result.unavailable}")

        budget = max(CAPACITY_FLOOR_SECONDS, result.processed * CAPACITY_SECONDS_PER_HEAD)
        if result.inferred and result.elapsed_seconds > budget:
            raise RuntimeError(
                f"enrichment took {result.elapsed_seconds:.0f}s for {result.processed} heads "
                f"({result.inferred} inferred), past the {budget:.0f}s bound — at this rate it "
                "will start lagging the 07:00 send (SPEC §7.3, ADR-0003)"
            )

        if result.rejected:
            # Printed, not raised. A model occasionally emitting something the schema refuses
            # is the *expected* steady state §7.3 built a quarantine table for; failing the
            # DAG over one bad decode would make a red run mean nothing. The rate is what
            # matters, and it is queryable in `gold.enrichment_rejects`.
            print(
                f"{result.rejected} of {result.processed} heads failed validation — "
                f"see {', '.join(('gold.enrichment_rejects',))}"
            )

        return {
            "processed": result.processed,
            "inferred": result.inferred,
            "cache_hit_rate": round(result.cache_hit_rate, 3),
            "rejected": result.rejected,
            "elapsed_seconds": round(result.elapsed_seconds, 1),
        }

    enrich()


enrich_dag()
