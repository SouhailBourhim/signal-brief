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
