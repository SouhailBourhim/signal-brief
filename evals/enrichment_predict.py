#!/usr/bin/env python3
"""Record the model's answers for the labeled set. SPEC §7.3; 4B.G.

Reads `evals/enrichment/examples.jsonl`, runs the pinned local model over each head, and
writes `evals/enrichment/predictions.jsonl` stamped with the `model_digest` and
`prompt_version` that produced them. `evals/score.py::score_enrichment` scores what this
records; it never calls the model itself.

**That split is not a convenience.** `make eval` gates every PR in CI, and CI has no GPU, no
Ollama, and no forty seconds to spare. Recording predictions is also what makes §7.3's
"accuracy tracked per model and prompt version" literally true: the stamp is on the row, so
swapping a model leaves the old numbers in place, visibly attributed to the old model, and
`score.py` declines to score them under the new one.

Schema failures are recorded rather than dropped — as a row with `schema_error` and no
prediction — because the README counts the schema-failure rate separately from accuracy, and
a dropped row would deflate the denominator of both.

    uv run python evals/enrichment_predict.py
    uv run python evals/enrichment_predict.py --limit 5   # smoke-test the wiring first
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pydantic import ValidationError

from signal_core.config import Settings
from signal_core.enrich import prompt as prompt_module
from signal_core.enrich.client import OllamaUnavailable, generate, supports_schema_format
from signal_core.enrich.schema import Enrichment

EVALS = Path(__file__).parent
EXAMPLES = EVALS / "enrichment" / "examples.jsonl"
PREDICTIONS = EVALS / "enrichment" / "predictions.jsonl"


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="only the first N examples")
    parser.add_argument("--examples", type=Path, default=EXAMPLES)
    parser.add_argument("--out", type=Path, default=PREDICTIONS)
    args = parser.parse_args(argv)

    settings = Settings()
    if settings.ollama_model_digest == "UNPINNED":
        print("Refusing to record predictions under an unpinned digest (ADR-0003).")
        print("Run `signal enrich --check-model` first.")
        return 1

    examples = _load(args.examples)[: args.limit]
    if not examples:
        print(f"No examples in {args.examples} — run `evals/sample_enrichment.py` first.")
        return 1

    schema = Enrichment.model_json_schema() if supports_schema_format(settings) else None
    print(
        f"{len(examples)} examples, {settings.ollama_model} @ {settings.ollama_model_digest[:19]}…"
    )
    print(f"structured output: {'schema-constrained' if schema else 'json mode'}")

    # Predictions under other digests/versions are kept. They are the record of what an
    # earlier model scored, which is the comparison §7.3 wants a model swap to be.
    keep = [
        row
        for row in _load(args.out)
        if not (
            row.get("model_digest") == settings.ollama_model_digest
            and row.get("prompt_version") == settings.prompt_version
        )
    ]

    rows, failures = [], 0
    started = time.monotonic()
    for index, example in enumerate(examples, start=1):
        rendered = prompt_module.render(
            title=example.get("title", ""),
            publisher=example.get("publisher_domain", ""),
            body=example.get("body", ""),
        )
        try:
            generation = generate(rendered, settings=settings, schema=schema)
        except OllamaUnavailable as exc:
            print(f"\nOllama unavailable after {index - 1} examples: {exc}")
            return 2

        row = {
            "input_hash": example["input_hash"],
            "cluster_id": example.get("cluster_id"),
            "model_digest": settings.ollama_model_digest,
            "model_name": settings.ollama_model,
            "prompt_version": settings.prompt_version,
            "elapsed_seconds": round(generation.elapsed_seconds, 2),
        }
        try:
            enrichment = Enrichment.model_validate_json(generation.text)
        except ValidationError as exc:
            failures += 1
            row["schema_error"] = str(exc)[:500]
            row["raw_output"] = generation.text[:1000]
        else:
            row["summary"] = enrichment.summary
            row["topic"] = enrichment.topic.value
            row["extraction"] = enrichment.extraction.model_dump()
        rows.append(row)
        print(f"  [{index}/{len(examples)}] {generation.elapsed_seconds:.1f}s", end="\r")

    elapsed = time.monotonic() - started
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for row in keep + rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    rate = failures / len(rows) if rows else 0.0
    print(f"\n{len(rows)} predictions in {elapsed:.0f}s ({elapsed / len(rows):.2f}s each)")
    print(f"schema-failure rate: {failures}/{len(rows)} = {rate:.1%}")
    print(f"wrote {args.out} ({len(keep)} older predictions kept)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
