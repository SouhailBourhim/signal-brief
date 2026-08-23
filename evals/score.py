#!/usr/bin/env python3
"""Scoring for the labeled evaluation sets. SPEC §7.1, §7.2, §7.3, §11.

Runs in CI on every PR. An accuracy regression fails the build, which is the mechanism
that makes SPEC's published precision/recall trustworthy rather than a claim made once.

The scorers call the pipeline's own decision functions. They never reimplement the rule —
an eval that scores a reimplementation measures a system nobody ships.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from signal_core.dedup import is_same_story
from signal_core.entities.resolve import resolve

EVALS = Path(__file__).parent


@dataclass
class Score:
    name: str
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    # Set when the confusion counts and the number of labeled examples are not the same
    # thing. `score_entities` counts a link to the wrong entity as both a false positive and
    # a false negative — two errors, one mention — so its cells sum to more than it scored.
    examples: int | None = None

    @property
    def support(self) -> int:
        if self.examples is not None:
            return self.examples
        return self.tp + self.fp + self.fn + self.tn

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) else 1.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if (self.tp + self.fn) else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    def line(self) -> str:
        return (
            f"{self.name:12} n={self.support:<5} "
            f"precision={self.precision:.3f} recall={self.recall:.3f} f1={self.f1:.3f} "
            f"(tp={self.tp} fp={self.fp} fn={self.fn})"
        )


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


FIXTURE_ORIGIN = "phase0-fixture"


def _score_dedup(name: str, *, fixture: bool) -> Score:
    """Same-story pair classification. SPEC §7.1.

    The two origins are scored separately and gated separately. Folding them into one
    number would let 55 synthetic pairs — correct by construction, since the fixture's
    `story_key` *is* the ground truth — mask roughly a fifth of the real set's failure
    headroom. They answer different questions: the fixture asks "does the harness still
    run", the real set asks "is the clustering any good".
    """
    score = Score(name)
    for pair in _load(EVALS / "dedup" / "pairs.jsonl"):
        if (pair.get("origin") == FIXTURE_ORIGIN) != fixture:
            continue
        predicted = is_same_story(
            pair["a"]["title"], pair["a"]["body"], pair["b"]["title"], pair["b"]["body"]
        )
        actual = pair["same_story"]
        if predicted and actual:
            score.tp += 1
        elif predicted and not actual:
            score.fp += 1
        elif not predicted and actual:
            score.fn += 1
        else:
            score.tn += 1
    return score


def score_dedup() -> Score:
    """The real labeled pairs. This is where the published number comes from."""
    return _score_dedup("dedup", fixture=False)


def score_dedup_fixture() -> Score:
    """The Phase 0 fixture, kept as a canary: it proves the scorer still runs."""
    return _score_dedup("dedup_fixture", fixture=True)


def dedup_by_stratum() -> list[Score]:
    """The real pairs, split by how they were sampled. SPEC §7.1, §11.

    Reporting one combined number for dedup would be misleading in a specific and
    avoidable way. `evals/sample_pairs.py` draws three base-rate-representative strata plus
    one (`focus`) deliberately enriched for the positive class, because same-story pairs are
    rarer than 1-in-60 in this corpus and recall over a base-rate sample would rest on
    almost no positives. Averaging an enriched sample into a representative one produces a
    figure that describes neither.

    So both get published: the representative strata say what the brief's reader actually
    sees, and `focus` says how the rule behaves once a plausible candidate is in front of it.
    """
    pairs = [
        pair
        for pair in _load(EVALS / "dedup" / "pairs.jsonl")
        if pair.get("origin") != FIXTURE_ORIGIN
    ]
    scores: dict[str, Score] = {}
    for pair in pairs:
        stratum = pair.get("stratum", "unsampled")
        score = scores.setdefault(stratum, Score(stratum))
        predicted = is_same_story(
            pair["a"]["title"], pair["a"]["body"], pair["b"]["title"], pair["b"]["body"]
        )
        actual = pair["same_story"]
        if predicted and actual:
            score.tp += 1
        elif predicted and not actual:
            score.fp += 1
        elif not predicted and actual:
            score.fn += 1
        else:
            score.tn += 1
    return [scores[name] for name in sorted(scores)]


# Where each set's labels live, so `main` can tell "nothing labeled yet" apart from
# "labeled, but nothing to score them against yet". Those are different states and only one
# of them is anybody's fault.
LABEL_FILES = {
    "dedup": EVALS / "dedup" / "pairs.jsonl",
    "dedup_fixture": EVALS / "dedup" / "pairs.jsonl",
    "entities": EVALS / "entities" / "mentions.jsonl",
    "enrichment": EVALS / "enrichment" / "examples.jsonl",
}


def score_entities() -> Score:
    """Mention-to-entity resolution. SPEC §7.2.

    Calls `entities.resolve.resolve` rather than reimplementing it, the same contract
    `score_dedup` keeps with `is_same_story`.

    **Abstention is a first-class answer.** 246 of the 300 labeled mentions are correctly
    unlinked, so a correct `unlinked` counts as a true negative. Without that, a resolver
    that links nothing looks perfect and so does one that links everything, depending which
    half you forgot to count.

    **A link to the wrong entity is counted twice — once as a false positive and once as a
    false negative.** It is two errors: a link that should not exist, and a link that should
    have. Counting it once, either way, would make a resolver that confidently mislabels
    every mention score better than one that abstains on the same mentions, which inverts
    exactly the preference SPEC §7.2's confidence floor exists to express.

    Mentions with no `entity_id` key are unanswered — not labeled `null` — and are skipped.
    """
    score = Score("entities", examples=0)
    for mention in _load(EVALS / "entities" / "mentions.jsonl"):
        if "entity_id" not in mention:
            continue
        score.examples = (score.examples or 0) + 1
        predicted = resolve(mention["surface_form"], mention.get("context", "")).entity_id
        actual = mention["entity_id"]
        if predicted == actual:
            score.tp += 1 if actual is not None else 0
            score.tn += 1 if actual is None else 0
        else:
            score.fp += 1 if predicted is not None else 0
            score.fn += 1 if actual is not None else 0
    return score


ENRICHMENT_FIELDS = ("company", "amount_usd", "round_type", "headcount_delta", "filing_type")


def _numbers(text: str) -> set[str]:
    """Digit runs, for the invented-figure check. Punctuation and separators are stripped so
    `$2.1 billion` and `2.1` compare, and `46,000` and `46000` do not read as different."""
    return {n.replace(",", "") for n in re.findall(r"\d[\d,]*(?:\.\d+)?", text or "")}


def score_enrichment() -> Score:
    """LLM output accuracy against labeled examples. SPEC §7.3.

    ## Why this scores recorded predictions rather than calling the model

    Every other scorer here calls the pipeline's own decision function — `is_same_story`,
    `resolve` — because those are deterministic and dependency-free, so CI can run them. This
    one cannot: it would need a GPU, a running Ollama, and ~40 seconds, none of which a CI
    runner has, and `make eval` gates every PR.

    So the model's answers are **recorded** by `evals/enrichment_predict.py` into
    `predictions.jsonl`, stamped with the `model_digest` and `prompt_version` that produced
    them, and this scores what was recorded. That is not a workaround — it is what §7.3's
    "accuracy tracked per model and prompt version" actually requires. Predictions stamped
    with a digest other than the one in `Settings` are **not scored**, and `main` says so:
    swapping the model invalidates the measurement rather than silently re-using it.

    ## Why the confusion matrix is over field decisions, not examples

    An example is seven decisions — one topic, five extraction fields, one summary — and they
    fail in different ways. Counting one example as one prediction would let a model that
    gets the topic right and every field wrong score the same as one that gets everything
    right but the topic.

    **Abstention is a first-class answer**, exactly as in `score_entities`: most extraction
    fields are correctly null (a story about a Go release has no round type), so a correct
    null is a true negative. Without that, an extractor that fills nothing looks perfect and
    so does one that fills everything, depending which half you forgot to count.

    **A wrong non-null value counts twice** — once as a false positive, once as a false
    negative. It is two errors: a value that should not be there, and one that should have
    been. Counting it once would make a model that confidently invents figures score better
    than one that abstains, which inverts the preference §7.3's whole typed-schema argument
    expresses.

    ## The summary rule

    `evals/enrichment/README.md`: "A fluent summary containing a number that appears nowhere
    in the source is a failure, not a near miss." That is mechanically checkable and it is
    checked here. Entailment in general is not, so the labeled `summary_ok` carries the human
    judgement and the invented-figure check runs on top of it — a summary can fail either
    way, and both count as the two-error case, because an invented figure is simultaneously a
    claim that should not exist and a correct summary that is missing.

    Schema-invalid output is **not** scored here. It is counted separately as the
    schema-failure rate (`gold.enrichment_rejects`), per the README.
    """
    settings_digest, settings_version = _enrichment_config()
    labels = {row["input_hash"]: row for row in _load(EVALS / "enrichment" / "examples.jsonl")}
    predictions = {
        row["input_hash"]: row
        for row in _load(EVALS / "enrichment" / "predictions.jsonl")
        if row.get("model_digest") == settings_digest
        and row.get("prompt_version") == settings_version
    }

    score = Score("enrichment", examples=0)
    for input_hash, label in labels.items():
        prediction = predictions.get(input_hash)
        if prediction is None:
            # Unmeasured, not wrong. Scoring it as a failure would mean a fresh clone with no
            # recorded predictions reported an accuracy of zero, which is a claim about a
            # model nobody ran.
            continue
        score.examples = (score.examples or 0) + 1

        if prediction.get("topic") == label.get("topic"):
            score.tp += 1
        else:
            score.fp += 1
            score.fn += 1

        summary = prediction.get("summary") or ""
        source = f"{label.get('title', '')} {label.get('body', '')}"
        invented = _numbers(summary) - _numbers(source)
        if label.get("summary_ok") and not invented:
            score.tp += 1
        else:
            score.fp += 1
            score.fn += 1

        predicted_fields = prediction.get("extraction") or {}
        actual_fields = label.get("extraction") or {}
        for field_name in ENRICHMENT_FIELDS:
            predicted = predicted_fields.get(field_name)
            actual = actual_fields.get(field_name)
            if predicted == actual:
                score.tp += 1 if actual is not None else 0
                score.tn += 1 if actual is None else 0
            else:
                score.fp += 1 if predicted is not None else 0
                score.fn += 1 if actual is not None else 0
    return score


def _enrichment_config() -> tuple[str, str]:
    """The digest and prompt version predictions must carry to be scored."""
    from signal_core.config import Settings

    settings = Settings()
    return settings.ollama_model_digest, settings.prompt_version


SCORERS = {
    "dedup": score_dedup,
    "dedup_fixture": score_dedup_fixture,
    "entities": score_entities,
    "enrichment": score_enrichment,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="score Signal's labeled eval sets")
    parser.add_argument("--gate", action="store_true", help="exit non-zero below threshold")
    parser.add_argument("--only", choices=sorted(SCORERS), help="score one set")
    parser.add_argument(
        "--by-stratum", action="store_true", help="break dedup down by how pairs were sampled"
    )
    args = parser.parse_args(argv)

    thresholds = tomllib.loads((EVALS / "thresholds.toml").read_text(encoding="utf-8"))
    names = [args.only] if args.only else list(SCORERS)

    failed = []
    for name in names:
        score = SCORERS[name]()
        limits = thresholds.get(name, {})
        if score.support == 0:
            labeled = len(_load(LABEL_FILES[name])) if name in LABEL_FILES else 0
            if name == "enrichment" and labeled:
                # A distinct message because the cause is distinct and actionable. Enrichment
                # scores *recorded* predictions (see `score_enrichment`), so "labeled but
                # unscored" here means nobody has run the model under the digest and prompt
                # version currently configured — which is exactly the state a model swap
                # should produce, rather than silently re-scoring the old model's answers.
                digest, version = _enrichment_config()
                recorded = len(_load(EVALS / "enrichment" / "predictions.jsonl"))
                print(
                    f"{name:12} {labeled} labeled, {recorded} predictions on file but none "
                    f"under {digest[:19]}… / {version} — run `evals/enrichment_predict.py`"
                )
                continue
            reason = (
                f"{labeled} labeled, awaiting a decision function to score against"
                if labeled
                else "no labeled examples yet"
            )
            print(f"{name:12} {reason} — not scored")
            continue

        print(score.line())
        for metric in ("precision", "recall"):
            floor = limits.get(f"min_{metric}")
            if floor is not None and getattr(score, metric) < floor:
                failed.append(f"{name}.{metric} {getattr(score, metric):.3f} < {floor}")

    if args.by_stratum:
        print("\ndedup by stratum (`focus` is enriched for positives — not a base rate):")
        for score in dedup_by_stratum():
            print("  " + score.line())

    if failed:
        print("\nFAILED:", *failed, sep="\n  ")
        return 1 if args.gate else 0
    print("\nall gates passed" if args.gate else "\nscored (no gate)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
