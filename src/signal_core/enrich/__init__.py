"""The governed LLM stage. SPEC §7.3; docs/runbooks/phase-4b.md.

Three outputs per cluster — a one-sentence summary, a topic, and a structured extraction —
produced by a pinned local model and governed like any other transform in this pipeline:
content-hash cache, schema validation, quarantine for what fails it, and an eval set that
makes swapping a model a measurement.

**Nothing in here belongs in the Lambda import chain.** Enrichment runs locally against
stored bytes, like every other interpretive stage (ADR-0002). It imports `httpx` and
`pydantic`, both of which the poller already carries, so `tests/test_lambda_artifact.py`
would not catch an accidental import — the separation is a design rule here, not something
the packaging test enforces for us.
"""
