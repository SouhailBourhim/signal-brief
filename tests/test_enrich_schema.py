"""The typed schema and the versioned prompt. SPEC §7.3; 4B.C.

These tests defend the two properties that make enrichment *governed* rather than just
called: an output that does not fit the schema cannot be stored, and a change to what the
model sees cannot happen without the cache key moving.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from signal_core.enrich import prompt as prompt_module
from signal_core.enrich.schema import (
    SUMMARY_MAX_CHARS,
    Enrichment,
    Extraction,
    Topic,
)
from signal_core.hashing import enrichment_cache_key

VALID = {
    "summary": "LG says a new OLED stack roughly doubles panel lifespan in lab testing.",
    "topic": "hardware-devices",
    "extraction": {"company": "LG"},
}


def test_a_well_formed_answer_validates_and_keeps_its_nulls():
    """`null` is the expected answer for most extraction fields, so it has to survive
    validation as a value rather than being coerced into something falsy-but-present."""
    enrichment = Enrichment.model_validate(VALID)

    assert enrichment.topic is Topic.HARDWARE_DEVICES
    assert enrichment.extraction.company == "LG"
    assert enrichment.extraction.amount_usd is None
    assert enrichment.extraction.round_type is None
    assert enrichment.extraction.headcount_delta is None
    assert enrichment.extraction.filing_type is None


def test_a_topic_outside_the_list_is_rejected_rather_than_stored():
    """`evals/enrichment/README.md` scores topic as exact match against the accepted-values
    list, which is only coherent if a value off the list cannot reach the table. A free-text
    topic column would turn the enrichment eval into a measurement of vocabulary drift."""
    with pytest.raises(ValidationError):
        Enrichment.model_validate({**VALID, "topic": "artificial-intelligence"})


def test_a_sixth_extraction_field_is_a_rejection_not_a_silent_drop():
    """`extra="forbid"`. A model returning a field this pipeline has nowhere to put and no
    eval for is not producing a richer answer — and dropping the field while storing the rest
    would mean the eval set scores a record the model did not actually produce."""
    with pytest.raises(ValidationError):
        Enrichment.model_validate(
            {**VALID, "extraction": {"company": "LG", "sentiment": "positive"}}
        )


def test_a_paragraph_is_not_a_one_sentence_summary():
    """§7.3 asks for one sentence. A model asked for one will occasionally produce five, and
    a brief whose stories are five sentences long is a different product — so the bound is
    enforced rather than requested."""
    with pytest.raises(ValidationError):
        Enrichment.model_validate({**VALID, "summary": "Long. " * SUMMARY_MAX_CHARS})


def test_an_empty_summary_is_rejected():
    """The failure mode a floor catches: a model that answers with punctuation or a stub
    still produces schema-valid JSON, and a brief of empty paragraphs looks like a rendering
    bug rather than an inference one."""
    with pytest.raises(ValidationError):
        Enrichment.model_validate({**VALID, "summary": "  "})


def test_summary_whitespace_is_normalized_so_the_same_answer_hashes_alike():
    """Trivial whitespace differences must not read as different summaries downstream."""
    enrichment = Enrichment.model_validate(
        {**VALID, "summary": "  LG  says a new  OLED stack doubles  panel lifespan.  "}
    )
    assert enrichment.summary == "LG says a new OLED stack doubles panel lifespan."


def test_extraction_defaults_to_all_nulls_when_the_model_omits_it():
    """A story about a Go release has no round type. The correct answer is an object of
    nulls, not a missing key that breaks validation for every non-funding story."""
    enrichment = Enrichment.model_validate({"summary": VALID["summary"], "topic": "ai-ml"})
    assert enrichment.extraction == Extraction()


def test_sec_filing_is_in_the_list_because_57_percent_of_the_corpus_is_one():
    """Measured before the enum was written (4B.A): 5,818 of 10,186 clusters are `sec.gov`,
    almost all routine fund administration. A taxonomy with no home for those files them
    under something editorial and makes the topic distribution a lie."""
    assert Topic.SEC_FILING in set(Topic)


def test_other_exists_so_the_model_can_decline():
    """A closed enum with no escape hatch does not produce better labels, it produces
    confident wrong ones — the same preference for abstention §7.2's confidence floor
    records."""
    assert Topic.OTHER in set(Topic)


# --- the prompt ---------------------------------------------------------------------------


def test_every_topic_reaches_the_prompt():
    """A value the schema accepts but the prompt never lists is one the model cannot choose,
    which would show up as an unexplained hole in the topic distribution rather than as an
    error."""
    rendered = prompt_module.render(title="t", publisher="p", body="b")
    for topic in Topic:
        assert topic.value in rendered


def test_body_is_truncated_so_a_long_feature_cannot_evict_the_instructions():
    """An 8B model at q4 has a bounded context, and when a full Ars feature plus the
    instructions overflow it, the instructions are what gets dropped. Truncating explicitly
    also keeps the cache key stable against a model's runtime configuration."""
    rendered = prompt_module.render(title="t", publisher="p", body="x " * 20_000)
    assert len(rendered) < prompt_module.BODY_MAX_CHARS + 2_000


def test_the_same_head_renders_identically_so_the_cache_can_hit():
    """The cache key is taken over this rendered string. Any nondeterminism here — a set
    iteration, a timestamp — would make every run a miss and the hit rate meaningless."""
    first = prompt_module.render(title="Same", publisher="verge.com", body="body text")
    second = prompt_module.render(title="Same", publisher="verge.com", body="body text")
    assert first == second


def test_changing_the_prompt_version_invalidates_the_cache_key():
    """The property `PROMPT_VERSION` exists for: editing the prompt without bumping it would
    serve cached output the current prompt would not have produced."""
    text = prompt_module.render(title="t", publisher="p", body="b")
    assert enrichment_cache_key(text, "sha256:abc", "v1") != enrichment_cache_key(
        text, "sha256:abc", "v2"
    )


def test_changing_the_model_digest_invalidates_the_cache_key():
    """ADR-0003's whole argument: swapping a model is a measurement, not a vibe. Serving one
    model's output under another's digest would make the eval scores unattributable."""
    text = prompt_module.render(title="t", publisher="p", body="b")
    assert enrichment_cache_key(text, "sha256:aaa", "v1") != enrichment_cache_key(
        text, "sha256:bbb", "v1"
    )
