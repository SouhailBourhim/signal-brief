# LLM enrichment labeled examples — SPEC §7.3

Empty until Phase 4B. Target 100 labeled examples, scored per model digest and prompt
version so swapping a model is a measurement rather than a vibe.

Unlike the dedup and entity sets, this one genuinely cannot start early: it labels
*clusters*, which do not exist until Phase 3 produces them.

## Labeling rule

For each cluster, record the correct one-sentence summary, topic, and structured
extraction. Score:

- **summary** — factually entailed by the source articles, no invented figures. A fluent
  summary containing a number that appears nowhere in the source is a failure, not a near
  miss.
- **topic** — exact match against the accepted-values list.
- **extraction** — field-level exact match, with `null` a valid and often correct answer.

Schema-invalid output is not scored here; it is counted separately as the schema-failure
rate and quarantined to `gold.enrichment_rejects`.

## The set, as labeled (5.0, 2026-08-29)

100 drawn, **95 distinct** — five stories were re-clustered into the sample under different
`cluster_id`s with identical head text, so they collapse on `input_hash`. `sample_enrichment.py`
now dedupes on the input before stratifying; the committed set is the pre-fix draw and is
sound, because the five duplicate pairs carry identical labels.

Strata: 20 SEC filings (the 20% cap), 40 corroborated, 40 single-source.

### Labeling rules, as actually applied

The README's three rules above are the contract. Three of them needed a tie-breaker that the
rule alone did not settle, and the calls are written down here so a second labeler can
reproduce or overturn them rather than guess:

- **`company` is read from the title and body, never from the publisher domain.** §7.3 says
  "as named in the text", and the domain is not the text. So a `github.com` post about a
  third-party project has `company: null`, and so does a Show HN whose product shares a name
  with its site. This is the rule that produces most of the model's 17 false positives, and it
  is deliberately strict: the alternative makes every self-published blog post a story about
  a company.
- **A product or project name is not a company.** `Conductor`, `TrueForge`, `Inkhaven`,
  `Runif`, `Qwen`, `ChatGPT`, `Arc` are all named in their titles and all labeled `null`. The
  field asks for the company, and inferring the vendor behind a product name is knowledge from
  outside the source.
- **`summary_ok` is false when the summary asserts something specific the source does not
  support** — an entity, a number, a causal claim, or a definition. Generic paraphrase passes.
  So "Researchers have developed drones with **retractable** claws **for grasping and
  manipulating objects**" fails against a source that says only "Drones with Claws", while "The
  article discusses the current state of the AI era, describing it as turbulent" passes.

Most bodies in this corpus are empty — HN submissions carry a title and a link — so the source
a summary must be entailed by is often the title alone. That is the honest question to score,
because it is the question the model was actually asked.

### Who labeled this

**An LLM assistant, unreviewed.** `labeler` is stamped on every record as
`claude-opus-5 (assistant), unreviewed`. The dedup and entity sets carry the same caveat with
one difference that matters: those were reviewed by the reader, who overrode three of them.
These have not been. It is a model grading a model — a much larger grader than the 8B being
graded, which is a common and defensible setup, but it is not ground truth. The README should
say so wherever it quotes the number, and a review pass is carried forward.
