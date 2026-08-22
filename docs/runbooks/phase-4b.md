# Phase 4B runbook — enrich + macro

Exit condition (SPEC §12): the Ollama enrichment stage with its content-hash cache, Pydantic
validation and 100-example eval set (§7.3), and the ALFRED bitemporal macro store (§8) —
accepted by **a 30-day backfill in which bronze bytes, normalization, hashing, simhash and
entity resolution reproduce identically; clustering reproduces within a stated tolerance given
a recorded ordering key; and enrichment resolves from cache with a published hit rate**.

Carried forward from 4A: **nothing blocking, one thing open.** 4A's own acceptance — three
mornings read with marks recorded — is calendar time and runs alongside this phase rather than
inside it. Its table lives in [`phase-4a.md`](phase-4a.md) and is filled in there, not here.

These are the two items ADR-0008 split Phase 4 to protect. They are §2's differentiators #3
and #4, and this phase builds them and nothing else.

## Decisions taken before starting

- **ADR-0009's three embedding items stay deferred** — the embedding branch behind
  `dedup.decide`, §7.4's novelty component, and the resolver's `?itemDescription` fix. The
  argument for pulling them in was real: ADR-0009 chose Ollama as the embedding provider, so
  once this phase stands the enrichment stage up, the infrastructure is largely present and
  the marginal cost looks small. It was declined anyway, because "the infrastructure is
  already there" is exactly the reasoning that produced the ten-item Phase 4 row ADR-0008 had
  to split. 4B carries two differentiators; a five-item 4B is the failure mode, not the
  ambitious version. **Consequence, stated rather than implied: the ranker stays five-sixths
  of §7.4 and dedup recall stays at ADR-0009's measured 0.500 ceiling for one more phase.**
- **The FRED key goes in SSM Parameter Store as a SecureString**, not a Lambda environment
  variable. This is the project's first real secret — every prior source authenticates with
  nothing, or with a User-Agent. An environment variable would put the key in Terraform state
  (which lives in S3) and in the Lambda console, for the saving of one boto3 call at cold
  start. Standard parameters are free and the AWS-managed `alias/aws/ssm` key carries no
  monthly charge, so the cheaper-looking option was not actually cheaper.
- **One Ollama call per cluster, not three.** §7.3 says "three jobs per cluster" and §9's
  schema settles how they run: `gold.cluster_enrichment` carries **one** `input_hash` and
  **one** `prompt_version` per `cluster_id`, so three independently-versioned calls do not fit
  the row that was specified for them. ADR-0003 measured the combined shape too — its ~1.0 s
  per head describes one call asking for all three outputs. Three calls would be ~3x the
  budget for a schema that cannot record which of them was which.
- **The reproducibility test is rehearsed now and gated later.** Bronze holds five days
  (below); the 30-day window cannot close before 2026-09-17. Rehearsing the harness on five
  days is not the acceptance and is not written up as though it were — but every prior phase
  found its real defects by running things rather than testing them, and five days finds them
  now instead of in four weeks. SPEC §12's wording is untouched; §18 names over-claiming
  reproducibility as a known failure mode and records that this test's wording deliberately
  survived the 4A/4B split.

## 4B.A — What the gates actually looked like

Three things gate this phase. All three were checked by probing rather than by reading a doc
that claimed something about them, which is 4A.A's lesson applied at the start instead of
after.

| Gate | Found | Cleared by |
|---|---|---|
| ADR-0003's model pin — `config.ollama_model_digest` still `"UNPINNED"`, and ADR-0003 says in as many words that Phase 4 cannot start before it lands | **Ollama not running.** `localhost`, `127.0.0.1`, the WSL2 default gateway `172.18.240.1` and `host.docker.internal`, all on :11434, all refused | Host-side: Ollama runs native for the GPU (ADR-0002) |
| FRED/ALFRED credentials | **A key is required.** The live endpoint answers `{"error_code":400,"error_message":"Variable api_key is not set"}` — checked before writing a line of the poller | A free key, then one `aws ssm put-parameter` |
| 30 days of bronze for the acceptance | **Five days.** `s3://signal-bronze-481879233905/staging/` spans `ingest_date=2026-08-18` through `2026-08-22` | Calendar — 2026-09-17 at the earliest |

The first two block *measurement*, not construction: every module here is testable against
`respx` and `moto` the way the rest of the repo is.

### The corpus is 57% SEC filings, and that reshapes the enrichment stage

Measured against the deployed lake before the stage was designed:

| Bucket | Clusters | Articles |
|---|---:|---:|
| `sec.gov` | **5,818** | 5,818 |
| other web (HN's outbound links, blogs, GitHub) | 3,957 | 4,018 |
| tech press (Verge, Ars, TechCrunch, Reuters, 404, NYT) | 411 | 479 |

**Every single SEC cluster has `article_count = 1`**, and a sample of eighty heads shows what
they are: `ABS-EE`, `N-PX`, `NPORT-P`, `424B2`, `485BXT`, `N-VP/A`, `144` — routine fund and
trust administration with no editorial content, which will never appear in a brief.

This matters because the obvious reading of §7.3 — "enrichment runs against cluster heads" —
would send **the majority of every inference budget to filings nobody will read**. ADR-0003
sized the capacity paragraph at "a 40-head batch," not a ten-thousand-head one, so the spec's
own measurement already assumed a bounded set.

So enrichment runs against **the ranked head of the window, not every cluster**. There is no
circularity in ordering it that way: §7.4's `WEIGHTS` has no enrichment component, so ranking
does not read what enrichment writes, and enriching after ranking spends the budget on exactly
the stories that will be read. This is recorded as a design decision rather than a detail
because the naive reading is the expensive one and it is right there in the spec's wording.

## 4B.B-G — The enrichment stage *(built 2026-08-22)*

- [x] `enrich/client.py` — `/api/generate` at temperature 0 with a fixed seed and a long
      `keep_alive`, so a batch pays ADR-0003's ~22.5 s model load once rather than forty times.
      Structured output is **probed, not assumed**: newer Ollama constrains decoding to a
      supplied JSON Schema, older builds accept only `format: "json"`, and the version
      numbering has not tracked the capability cleanly. Falls back, and validates on our side
      either way.
- [x] `enrich/schema.py` — Pydantic `Enrichment` with `extra="forbid"` on both levels, a
      closed `Topic` enum, and the five extraction fields §7.3 names, every one nullable.
- [x] `enrich/prompt.py` — `PROMPT_VERSION` participates in the cache key, so editing the
      prompt without bumping it would serve output the current prompt would not have produced.
- [x] `enrich/store.py` — `gold.cluster_enrichment` and `gold.enrichment_rejects` through
      Athena, with `information_schema` reconciliation on every run (see below).
- [x] `enrich/run.py` — cache, retry bound, quarantine, and the run-level hit rate.
- [x] `airflow/dags/enrich_dag.py` at 06:15, asserting §7.3's capacity bound.
- [x] `evals/score.py::score_enrichment`, `evals/sample_enrichment.py`,
      `evals/enrichment_predict.py`.
- [ ] The 100 labeled examples and the ratcheted floors — **blocked on Ollama**, below.

### The topic list is fitted to the corpus, not borrowed

`evals/enrichment/README.md` scores topic as "exact match against the accepted-values list",
which is only coherent if the list is closed — and a closed list has to be closed around
something real. It was drawn after reading eighty actual cluster heads, and two findings
shaped it:

- **`sec-filing` exists because 57% of clusters are one.** A taxonomy without a home for
  `ABS-EE` and `N-PX` files them under something editorial and makes the topic distribution a
  lie.
- **The editorial half is HN and tech press, not business news.** A taxonomy led by
  `earnings` / `m-and-a` / `funding` would mislabel most of this corpus, because most of it is
  people shipping software, breaking software, or writing about models.

`other` is deliberate and load-bearing: a closed enum with no escape hatch does not produce
better labels, it produces confident wrong ones — the same preference for abstention §7.2's
confidence floor records.

### Scoring records predictions rather than calling the model

Every other scorer in `evals/` calls the pipeline's own decision function, because
`is_same_story` and `resolve` are deterministic and dependency-free. This one cannot: it needs
a GPU, a running Ollama and ~40 seconds, and `make eval` gates every PR in CI.

So `enrichment_predict.py` records answers stamped with the digest and prompt version that
produced them, and `score.py` scores what was recorded — declining, loudly, to score one
model's answers under another's pin. That is not a workaround for CI; it is what §7.3's
"accuracy tracked per model and prompt version" actually requires, and it makes a model swap a
visible event rather than a silent re-scoring.

The confusion matrix is over **field decisions, not examples**: one example is seven decisions
(a topic, five extraction fields, a summary), and counting an example as one prediction would
let a model that gets the topic right and every field wrong score the same as one that gets
everything right but the topic. Abstention counts as a true negative and a wrong non-null
counts twice, both exactly as `score_entities` already does — without that, an extractor that
fills nothing looks perfect and so does one that fills everything, depending which half you
forgot to count.

## 4B.H-J — The bitemporal macro store *(built 2026-08-22)*

- [x] `infra/terraform/main/macro.tf` — SSM `SecureString` created with a placeholder and
      `ignore_changes = [value]`, plus a poller grant scoped to that one parameter ARN.
- [x] `sources/macro.py` — source #9, resolving the key from SSM at fetch time and caching it
      per container.
- [x] `parse/macro.py` — ALFRED's flat observations array into `ParsedMacroObservation`.
- [x] `spark/jobs/macro.py` — `gold.macro_observations`, insert-only MERGE plus a whole-table
      recompute of `is_latest` and `revision_delta`.
- [x] `airflow/dags/macro_dag.py` at 03:40, its own cron for the reason `market_dag` has one.
- [x] `brief/read.py::read_macro_revisions` and the brief's revision block.
- [ ] A real fetch — **blocked on the FRED key**, below.

### Three payload details that would each have been a silent wrong answer

- **`value` is a string and `"."` means missing.** It becomes `None`, not `0.0`. A zero
  unemployment rate is a very different claim from an unpublished one, and a `revision_delta`
  computed against a zero-filled gap would report a fictional revision the size of the whole
  series.
- **`realtime_end` of `9999-12-31` means "still current"**, not a date in the year 9999. It
  becomes `superseded_at = None`, because a sentinel that survives into the table is one
  somebody eventually does arithmetic on.
- **FRED never echoes the series id in the response body.** It is a property of the request,
  so it is recovered from the stored `source_url` — which means it comes out of the immutable
  record rather than from mutating the payload before storing it, which SPEC §6.1 forbids. A
  row whose id cannot be recovered is dropped rather than landing under an empty series id and
  merging six unrelated series into one.

### `is_latest` and `revision_delta` are recomputed over the whole table

Not the incoming batch. A new vintage for June demotes June's previous `is_latest` and gives
the new row a delta against it — and the demoted row may not be in today's window at all.
Scoping the recompute to the batch would leave two rows claiming to be current, which is the
single most damaging thing a bitemporal store can get wrong, because every "what is the number
now" query then silently doubles. `test_a_later_vintage_demotes_a_row_the_batch_never_touched`
is that scenario.

A first vintage has a **null** delta, not zero. "Not yet revised" and "revised by zero" are
different facts, and §8's whole argument is that collapsing facts about revisions is how
pipelines lose them.

### The vintage window is bounded, and the bound is a decision

`REALTIME_START` is 2015-01-01 rather than each series' first observation. §8's worked payoff
is "payrolls revised down 46k across the prior two months" — a claim about *recent* revisions
— and a full vintage history of PAYEMS back to 1939 is a much larger response for data no
brief will cite. It also stays well inside FRED's 100,000-observation per-request ceiling,
which a full daily series across all vintages could plausibly approach. Widening it is a
one-line change; the parser reports a truncated response rather than trusting it either way.

## 4B.K — The reproducibility harness *(built 2026-08-22)*

`ops/reproduce.py`, plus `signal reproduce --days N`. SPEC §12's 4B gate is **three different
claims**, and the harness keeps them apart because SPEC §18 names over-claiming reproducibility
as a known failure mode — the easy version of this module, one boolean called `reproducible`,
is precisely that over-claim.

| Stage | Claim | Gates? |
|---|---|---|
| bronze bytes | identical | yes — a mismatch is storage corruption |
| normalize + hashing + simhash | identical | yes — pure functions of stored bytes |
| entity resolution | identical | yes — a dictionary lookup at a fixed floor |
| clustering | agreement ≥ 0.95, given the recorded ordering key | yes |
| enrichment | resolves from cache | **no — it publishes a rate** |

Two decisions worth flagging:

**Clustering is compared as a partition, not by cluster id.** Two runs that group the same
articles together but name the groups differently have reproduced the clustering; comparing
ids would call that a failure and make the number describe id generation rather than the
algorithm. Agreement is the fraction of articles whose set of co-members is identical.

**The enrichment stage never fails.** §12 asks for "a published hit rate", not a floor, and
inventing one would be a claim the spec did not make — and would fail an acceptance test about
determinism on a cold cache. The model is not reproducible; the cache is, and that distinction
is the whole reason §12 words that clause differently from the other two.

Each stage replays into a **shadow table** under the `repro` namespace. Re-running into the
live tables would make the test pass by overwriting the thing it was meant to check.
