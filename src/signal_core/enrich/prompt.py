"""The versioned prompt. SPEC §7.3's "versioned prompts" half of the determinism boundary.

`PROMPT_VERSION` participates in the cache key (`hashing.enrichment_cache_key`), so editing
anything in this module without bumping it serves cached output the current prompt would not
have produced — which would make cache-hit rate a number about the past rather than a metric.
That is the whole reason the version is a constant here rather than a setting someone can
forget to change.

**Bump `PROMPT_VERSION` for any edit that changes what the model sees**, including whitespace
inside `TEMPLATE`, and record what changed and why in `docs/runbooks/phase-4b.md`. The eval
set is scored per model digest and prompt version precisely so that a bump is a measurable
event rather than a silent one.
"""

from __future__ import annotations

from signal_core.enrich.schema import SUMMARY_MAX_CHARS, Topic

# v1 is the first real prompt. `Settings.prompt_version` shipped as "v0" through Phase 0-4A,
# when nothing called a model and the value was a placeholder.
PROMPT_VERSION = "v1"

_TOPICS = "\n".join(f"- {topic.value}" for topic in Topic)

# Body text is truncated before it reaches the model. Cluster heads run from a two-line HN
# submission to a full Ars feature, and an 8B model at q4 has a context budget that a long
# feature plus the instructions will exhaust — at which point the instructions are what gets
# evicted. Truncating explicitly makes the input bounded and the cache key stable; letting
# the context window do it makes both depend on the model's runtime configuration.
BODY_MAX_CHARS = 4000

TEMPLATE = """\
You are labelling one news story for a personal daily brief. Answer only with JSON.

Return an object with exactly these keys:
  "summary"    - ONE sentence, at most {summary_max} characters, stating what happened.
                 Every fact in it must appear in the text below. Do not add figures,
                 dates, or company names that are not in the text.
  "topic"      - exactly one of:
{topics}
  "extraction" - an object with exactly these keys, using null when the text does not
                 state the value. Null is the correct answer far more often than not.
      "company"         - the primary company named, or null
      "amount_usd"      - a number of US dollars, or null
      "round_type"      - e.g. "Seed", "Series A", or null
      "headcount_delta" - jobs added as a positive integer, jobs cut as a negative
                          integer, or null
      "filing_type"     - the SEC form type such as "8-K", "4", "D", or null

Guidance that matters more than it looks:
- Routine SEC administrative filings (ABS-EE, N-PX, NPORT-P, 424B2, 485BXT, 144) are
  topic "sec-filing". They are not business news and should not be labelled as such.
- If nothing in the list fits, answer "other". A wrong confident label is worse than
  "other".

TITLE: {title}
PUBLISHER: {publisher}

TEXT:
{body}
"""


def render(*, title: str, publisher: str, body: str) -> str:
    """The exact string the model sees, and the exact string the cache key is taken over.

    `enrichment_cache_key` hashes this rendered text rather than the cluster id, so two
    clusters whose head text is identical share one inference — and the same cluster whose
    head was edited gets a new one. That is the correct behaviour in both directions and it
    falls out of hashing the input instead of the identifier.
    """
    return TEMPLATE.format(
        summary_max=SUMMARY_MAX_CHARS,
        topics=_TOPICS,
        title=(title or "").strip(),
        publisher=(publisher or "").strip() or "unknown",
        body=" ".join((body or "").split())[:BODY_MAX_CHARS],
    )
