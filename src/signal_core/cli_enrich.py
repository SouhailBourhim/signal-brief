"""`signal enrich` and `signal enrich --check-model`. SPEC §7.3; ADR-0003; 4B.B.

Two verbs behind one subcommand because they answer the two questions this stage raises:
"is the pin still true" and "what did the batch do".

`--check-model` is the one that matters at a phase boundary. ADR-0003 says Phase 4 cannot
start until `ollama_model_digest` is pinned, and pinning it means **verifying** it — the ADR
recorded a digest on 2026-08-18, and a re-pull of the `llama3.1:8b` tag since then would move
it. A digest copied out of a document rather than read off the running box makes the cache
key look trustworthy while keying on a fiction, which is worse than `UNPINNED`, because
`UNPINNED` is at least visibly wrong.
"""

from __future__ import annotations

from signal_core.config import Settings
from signal_core.enrich.run import UNPINNED


def run_check_model(settings: Settings | None = None) -> int:
    """Compare the configured pin against the digest Ollama actually has installed.

    Exit codes are meaningful, so this can gate a deploy: 0 agrees, 1 disagrees or the pin is
    missing, 2 could not tell because the server is unreachable. "Could not tell" and "wrong"
    are different answers and collapsing them would let an unreachable server read as a pass.
    """
    from signal_core.enrich.client import local_model_digest

    settings = settings or Settings()
    local = local_model_digest(settings)

    print(f"model    {settings.ollama_model}")
    print(f"pinned   {settings.ollama_model_digest}")
    print(f"local    {local or '(unreachable)'}")

    if local is None:
        print(
            f"\nCannot verify: {settings.ollama_url} did not answer. Ollama runs natively on "
            "the host, not in Compose (ADR-0002) — start it there, then re-run."
        )
        return 2
    if settings.ollama_model_digest == UNPINNED:
        print(
            f"\nNot pinned. ADR-0003 requires a digest before enrichment runs. Set\n"
            f"  SIGNAL_OLLAMA_MODEL_DIGEST={local}\n"
            "in .env (or Settings.ollama_model_digest) and record the measurement in the ADR."
        )
        return 1
    if settings.ollama_model_digest != local:
        print(
            "\nDRIFT: the installed model is not the pinned one. Either the tag was re-pulled "
            "or the pin is stale.\nDo not silently repin — ADR-0003 records digests as "
            "measurements, so record this one as a second measurement with its date, and note "
            "that every cached enrichment under the old digest is now correctly invalid."
        )
        return 1

    print("\nPinned digest matches the installed model.")
    return 0


def run_enrich(*, limit: int | None = None) -> int:
    """Run the stage over the ranked head of the window and print what it did."""
    from signal_core.enrich.run import ENRICH_TOP_N, run

    settings = Settings()
    if settings.ollama_model_digest == UNPINNED:
        # Refused rather than run. Every row written under an unpinned digest is one the
        # cache can never legitimately serve, because the key it was written under does not
        # describe a model — so this would spend GPU time producing garbage cache entries.
        print(
            "Refusing to enrich with an unpinned model digest (ADR-0003).\n"
            "Run `signal enrich --check-model` to see the installed digest and pin it."
        )
        return 1

    result = run(limit=limit or ENRICH_TOP_N, progress=print)
    print(
        f"{result.processed} heads: {result.inferred} inferred, "
        f"{result.cache_hits} from cache ({result.cache_hit_rate:.0%}), "
        f"{result.rejected} quarantined, {result.skipped_exhausted} past the retry bound"
    )
    print(f"{result.written} rows written in {result.elapsed_seconds:.1f}s")

    if result.unavailable:
        print(f"\nOllama unavailable, batch stopped early: {result.unavailable}")
        return 1
    return 0
