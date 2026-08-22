# ADR-0010 — Stooq for market data, SES from the local side, novelty deferred to 4B

**Status:** Accepted · **Date:** 2026-08-22

## Context

Phase 4A completes SPEC §7.4's ranker. Four of its six components are wiring over data the
pipeline already holds. Two are not, and both force a choice this project has to record
rather than absorb:

- **Market corroboration** — "did the linked ticker or rate move beyond its normal range?"
  There is no price data in the lake. SPEC §3 lists Stooq and yfinance below the line as
  "Phase 4+" without picking one.
- **Novelty** — "embedding distance to the last 30 days of clusters." ADR-0009 adopted
  embeddings for same-story dedup **in 4B, via Ollama**, and rejected them for entity
  resolution outright. It did not say what novelty does in the meantime.

Separately, 4A's deliverable is a brief *emailed at 07:00*, and the project has no mailer.
ADR-0002 splits the runtime — ingestion serverless in AWS because it must run whether or not
the laptop is on, everything interpretive local — and the send has to land on one side of
that line.

## Decision

### 1. Market data comes from Stooq, and the choice is a packaging constraint

yfinance is the better-known library and the worse fit here. It pulls **pandas** transitively,
and `tests/test_lambda_artifact.py` fails the build if the poller handler's import chain
acquires any of `("pyarrow", "pyspark", "pandas", "numpy", "jinja2")` — the assertion that
enforces ADR-0006's 250 MB unzipped ceiling. A market poller is a Lambda like the other seven,
so its parser lives in the same import chain as the rest, and adding pandas to reach it would
either break that test or force the market source out of the shape every other source has.

Stooq serves daily OHLCV as plain CSV over HTTP. The poller is an `httpx` GET and the parser
is `csv` from the standard library — nothing new in `pyproject.toml`'s `lambda` extra, and the
source implements the same `poll(config, state) -> (list[RawDocument], State)` contract as
every other. **The library that needed no dependency won on the constraint the architecture
already had**, not on data quality; if Stooq's coverage turns out to be the binding problem,
the re-entry criterion is a *fetch* change, because the contract keeps parsing on the far
side of stored bytes where it can be redone against the archive.

Observations land in `silver.market_observations`, not gold. SPEC §9 lists `macro_observations`
under gold, but that classification is about a **bitemporal** store with `valid_time`/`known_time`
axes — §8's ALFRED work, 4B. Stooq OHLCV is a straight parse of one source's bytes, structurally
`silver.hn_comments`: a second table off a single source, not a cross-source aggregate. It is
written by a MERGE that **updates on match** rather than appending, because Stooq restates
history for splits and dividends, and a re-fetch that corrects an old row has to overwrite it.

### 2. The brief is mailed from the local side, via SES

ADR-0002's boundary decides this. The renderer runs locally and holds the finished HTML in
memory; a Lambda mailer would exist only to re-read from S3 what the process that called it
just produced, and would put the 07:00 send behind a deployment cycle. `brief/mailer.py`
sits beside `ranker.py` and `render.py` — which is also where SPEC §13's repository layout
puts it (`brief/ # ranker, renderer, mailer`).

SES over SMTP because the credentials are already there: `~/.aws` is what `ops/athena.py`
authenticates every brief query with, so the mailer adds an IAM action rather than a secret.
Gmail SMTP would need an app password — a long-lived credential in a project whose CI
deliberately runs on OIDC with no static AWS keys (ADR-0005), stored somewhere for a daily
job to read.

**The identity stays in the SES sandbox.** Sender and recipient are the same verified address:
this is a brief one person reads. Requesting production access buys the ability to mail
strangers, which is not a thing this system should be able to do.

The IAM grant is a dedicated least-privilege role even though `Souhail_Signal_Admin` already
holds `AdministratorAccess` and could send today with no Terraform at all. That is the same
argument `query.tf` already makes for `signal-analyst` — *"'I query with an admin key' undoes
least-privilege even when the identity behind it is trustworthy"* — and the same reason it
applies to a mailer, which unlike a query has an outward-facing side effect.

### 3. Novelty is deferred to 4B, and `WEIGHTS` says so

ADR-0009 priced `sentence-transformers` at 1.1 GB installed and declined to pay it in Phase 3,
routing the same capability through Ollama in 4B where a cache, a model pin and cost accounting
already exist. Novelty needs exactly that capability — embedding distance against 30 days of
cluster heads — so building it in 4A means either paying ADR-0009's bill one phase early, or
standing up a second, lesser encoder beside the one 4B will bring.

A lexical proxy was considered and rejected. Novelty asks whether a *narrative* is recycled,
and the sharpest evidence in this repo says lexical similarity cannot answer that: ADR-0009
measured held-out recall of 0.500 for the lexical same-story rule against 0.909 for embeddings
on a question that is strictly easier. A proxy scoring near-chance would still occupy the
weight, and a hand-set weight over a near-chance component is worse than an absent one,
because the score stays explainable only if every component in it means something (§7.4).

So `WEIGHTS` ships with five of six components and a comment naming the sixth, matching how
the module already documented `velocity` while its poller did not exist.

## Consequences

**Source count goes to eight, and one test knows the number.**
`tests/test_source_registry.py::test_phase_2_reaches_six_deployed_sources` asserts
`len(DEPLOYED_SOURCE_IDS) == 6` as a literal. The HN score poller and Stooq both land in 4A,
so it becomes 8. Worth noting that this assertion is the only thing that would notice a source
being added or dropped silently, which is why it is a literal rather than a length check
against the config it is checking.

**Market corroboration ships with a stated threshold, not a fitted one.** Corroborated means
`abs(latest daily return) > 1.5 x trailing 20-day return stddev` for the ticker of the
cluster's highest-mention watchlist entity. There is no labeled set for "the market reacted"
the way there is for dedup and entities, and SPEC §7.4 argues for explainable constants over
tuned ones. The number is a decision to revisit against data, and it is written down so that
revisiting it is a comparison rather than an excavation.

**The published dedup recall stays 0.500 through 4A** — ADR-0009 already said this, and
nothing in 4A changes it. 4A.G touches `dedup.decide` to fix EDGAR cross-CIK clustering, but
it adds a positive rule *ahead of* the identifier veto rather than loosening the veto, because
ADR-0009 recorded that the veto "is now load-bearing for a stage that does not exist yet."
`make eval` reporting unchanged precision and recall after that change is the expected result
and the reason to run it.

**What would reverse the Stooq choice.** Coverage. The watchlist is small and US-listed today;
a watchlist that needs non-US tickers, intraday data, or corporate-action detail Stooq's CSV
does not carry is a different problem, and the poller contract is what keeps that a
one-module change.
