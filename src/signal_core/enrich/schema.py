"""The typed shape an enrichment must land in. SPEC §7.3, §9.

SPEC §7.3 asks for "structured extraction ... into a typed schema" with "Pydantic validation
on every output", and for failures to be "quarantined to `gold.enrichment_rejects`, never
silently dropped, and never retried indefinitely". This module is the schema half of that;
`reject.py` is the quarantine half.

## Why the topic list is a closed enum

`evals/enrichment/README.md` scores topic as "exact match against the accepted-values list",
which is only a coherent rule if the list is closed. An open string field would score every
near-miss as a failure (`ai` vs `ai-ml` vs `artificial-intelligence`) and would make the
enrichment eval a measurement of vocabulary drift rather than of classification.

## Why *these* values

They are fitted to what the corpus actually contains, sampled from `silver.story_clusters`
before the enum was written (docs/runbooks/phase-4b.md 4B.A) rather than borrowed from a
general news taxonomy. Two findings shaped it:

- **57% of clusters are SEC filings**, almost all routine fund and trust administration —
  `ABS-EE`, `N-PX`, `NPORT-P`, `424B2`. A taxonomy without a home for those files them under
  something editorial and makes the topic distribution a lie. Hence `SEC_FILING`, which is a
  statement that a document is administrative rather than a claim about its subject.
- **The editorial half is HN and tech press, not business news.** A taxonomy led by
  `earnings` / `m-and-a` / `funding` would mislabel most of it, because most of it is people
  shipping software, breaking software, or writing about models. Hence the weight on
  `AI_ML`, `SOFTWARE_ENGINEERING` and `SECURITY`.

`OTHER` is deliberate and load-bearing. A closed enum with no escape hatch does not produce
better labels, it produces confident wrong ones — and §7.2's confidence floor already
records this project's preference for abstention over a plausible mislabel.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

# Bounds on the summary, enforced rather than requested. §7.3 asks for "a one-sentence
# summary"; a model asked for one sentence will occasionally produce five, and a brief whose
# stories are five sentences long is a different product. The ceiling is generous enough that
# a genuinely long sentence survives and tight enough that a paragraph does not.
SUMMARY_MAX_CHARS = 400
SUMMARY_MIN_CHARS = 20


class Topic(StrEnum):
    """Accepted values, fitted to the corpus. See the module docstring."""

    AI_ML = "ai-ml"
    SECURITY = "security"
    SOFTWARE_ENGINEERING = "software-engineering"
    HARDWARE_DEVICES = "hardware-devices"
    SCIENCE_RESEARCH = "science-research"
    BUSINESS_CORPORATE = "business-corporate"
    POLICY_REGULATION = "policy-regulation"
    SEC_FILING = "sec-filing"
    SOCIETY_CULTURE = "society-culture"
    OTHER = "other"


class Extraction(BaseModel):
    """SPEC §7.3's five extraction fields.

    **Every field is nullable and null is the expected answer most of the time.**
    `evals/enrichment/README.md` already committed to this — "field-level exact match, with
    `null` a valid and often correct answer" — and it is the difference between an extractor
    and a hallucination generator. A story about a Go release has no round type; a model
    pressured to fill the field will invent one, and §7.3's whole argument for a typed schema
    is that invented values should be impossible to record rather than merely unlikely.
    """

    model_config = {"extra": "forbid"}

    company: str | None = Field(default=None, description="Primary company, as named in the text")
    amount_usd: float | None = Field(default=None, description="Money amount in USD, if stated")
    round_type: str | None = Field(default=None, description="Seed, Series A, etc., if stated")
    headcount_delta: int | None = Field(
        default=None, description="Jobs added (positive) or cut (negative), if stated"
    )
    filing_type: str | None = Field(default=None, description="SEC form type, e.g. 8-K, 4, D")


class Enrichment(BaseModel):
    """One cluster's enrichment, validated. The shape `gold.cluster_enrichment` stores.

    `extra="forbid"` on purpose: a model that returns a sixth extraction field is not
    producing a richer answer, it is producing an answer this pipeline has nowhere to put and
    no eval for. Quarantining it is more honest than dropping the field and storing the rest,
    because the eval set would then score a record the model did not actually produce.
    """

    model_config = {"extra": "forbid"}

    summary: str = Field(description="One sentence, factually entailed by the source text")
    topic: Topic
    extraction: Extraction = Field(default_factory=Extraction)

    @field_validator("summary")
    @classmethod
    def _one_reasonable_sentence(cls, value: str) -> str:
        text = " ".join(value.split())
        if not SUMMARY_MIN_CHARS <= len(text) <= SUMMARY_MAX_CHARS:
            raise ValueError(
                f"summary must be {SUMMARY_MIN_CHARS}-{SUMMARY_MAX_CHARS} chars, got {len(text)}"
            )
        return text
