# ADR-0011 — What enrichment runs over, how it is scored, and where the first secret lives

**Status:** Accepted · **Date:** 2026-08-22

## Context

Phase 4B builds SPEC §7.3's governed LLM stage and §8's bitemporal macro store — §2's
differentiators #3 and #4. Three decisions came up that are not obvious from the spec, that
change what the code does, and that a reader would otherwise have to reverse-engineer.

---

## 1. Enrichment runs over the *ranked head* of the window, not every cluster

### Context

SPEC §7.3 says enrichment runs "against cluster heads once per pre-brief window". Read
literally, that is every cluster in the window.

Measured against the deployed lake **before the stage was designed**:

| Bucket | Clusters | Articles |
|---|---:|---:|
| `sec.gov` | **5,818** | 5,818 |
| other web (HN outbound links, blogs, GitHub) | 3,957 | 4,018 |
| tech press (Verge, Ars, TechCrunch, Reuters, 404, NYT) | 411 | 479 |

**57% of clusters are SEC filings, and every one is a single-article cluster.** A sample of
eighty heads shows what they are: `ABS-EE`, `N-PX`, `NPORT-P`, `424B2`, `485BXT`, `N-VP/A`,
`144` — routine fund and trust administration with no editorial content, which will never
appear in a brief.

### Decision

Enrichment runs over the top `ENRICH_TOP_N = 40` clusters of the same ranked window the brief
renders, via `brief/select.py::ranked_window`.

### Consequences

- **The literal reading would spend the majority of every inference budget on documents
  nobody reads.** ADR-0003's capacity paragraph already assumed a bounded set — it sizes the
  measurement at "a 40-head batch", not a ten-thousand-head one — so the spec's own
  measurement was never consistent with the literal reading of its own prose.
- **No circularity.** §7.4's `WEIGHTS` has no enrichment component and 4B did not add one, so
  ranking never reads what enrichment writes.
- **A margin, not an exact match.** 40 is four times the brief's default cut of 10, which
  absorbs any ranking drift between the 06:15 enrich run and the 07:00 send. A cluster that
  slips through unenriched degrades to its snippet rather than breaking the brief.
- **Ranking is now shared code.** `brief/build.py` and `enrich/run.py` call one function;
  two copies of the read sequence would drift the first time a component was added to
  `WEIGHTS`, and then enrichment would be spending its budget on a different set of stories
  than the brief shows.

### What would reverse this

A corpus where the filings *are* the product — a compliance brief rather than a news one — or
a model cheap enough that enriching ten thousand heads costs nothing. Neither is true at
~1.0 s per head on one consumer GPU.

---

## 2. The enrichment eval scores *recorded predictions*, not live inference

### Context

Every other scorer in `evals/` calls the pipeline's own decision function — `is_same_story`,
`resolve` — because those are deterministic and dependency-free. `make eval` gates every PR
in CI, and CI has no GPU, no Ollama, and no forty seconds to spare.

### Decision

`evals/enrichment_predict.py` runs the model and writes `predictions.jsonl`, stamped with the
`model_digest` and `prompt_version` that produced each answer. `evals/score.py` scores what
was recorded, and **declines to score predictions carrying a different stamp**.

### Consequences

- This is what §7.3's "accuracy tracked per model and prompt version" literally requires. A
  model swap leaves the old numbers in place, visibly attributed to the old model, and
  `make eval` reports "no predictions under the current pin" rather than silently re-scoring
  yesterday's answers under today's digest.
- **The eval measures a recording, not a live system.** That is a real limitation and it is
  the same class of caveat 3.B recorded for dedup ("the pairwise eval cannot certify the
  clustering"). A prediction file can go stale against a prompt edit; the digest and version
  stamp are what make that detectable rather than invisible.
- The confusion matrix is over **field decisions, not examples** — one example is seven
  decisions. Abstention counts as a true negative and a wrong non-null counts twice, exactly
  as `score_entities` already does, because without that an extractor that fills nothing
  looks perfect and so does one that fills everything.

---

## 3. The FRED key lives in SSM Parameter Store

### Context

ALFRED is the first source in this project that needs a secret. Every prior source
authenticates with nothing, or with a User-Agent that identifies the operator rather than
proving anything. Confirmed against the live endpoint before the poller was written: an
anonymous request answers HTTP 400 with `Variable api_key is not set`.

### Decision

An `aws_ssm_parameter` of type `SecureString` under the AWS-managed `alias/aws/ssm` key,
created by Terraform with the placeholder `UNSET` and `lifecycle { ignore_changes = [value] }`.
The poller resolves it at fetch time with a per-container cache; the poller role gets
`ssm:GetParameter` on that one parameter ARN plus `kms:Decrypt` constrained by `ViaService`.

### Alternatives

**A Lambda environment variable** was the tempting option: no new IAM, no extra call, one line
in `lambda.tf`'s existing `environment` block. It would also put the key in Terraform state —
which lives in an S3 bucket — and render it in plaintext in the Lambda console. For the saving
of one `GetParameter` at cold start, that is a bad trade.

**Secrets Manager** is the purpose-built service and costs $0.40 per secret per month plus API
calls, against $0 here. Its distinguishing features are rotation and cross-account sharing,
and a free personal API key needs neither. SPEC §10 treats the free tier as a design
constraint rather than a preference, so the paid service has to earn the line item.

### Consequences

- **Terraform owns the parameter's existence and its IAM scoping; it never owns the secret.**
  Putting the real key in a `.tfvars` would defeat the whole point, since it would land in
  state either way.
- **A human step, like `mail.tf`'s SES verification.** Terraform creates it pending and a
  person completes it with one `aws ssm put-parameter`. `sources/macro.py` matches the
  placeholder string exactly so the poller can say "still holds the Terraform placeholder"
  rather than passing `UNSET` to FRED and surfacing its generic rejection.
- **The key never reaches bronze.** `source_url` is reconstructed from the parameters that
  matter rather than copied from `response.request.url`. Bronze is immutable (SPEC §6.2), so
  a secret written into it could not be redacted later — only the whole object deleted.
- **A per-container cache means a rotated key takes effect on the next cold start**, not
  immediately. Stated here because "why did the old key keep working for ten minutes" is the
  question this will eventually raise.
- If a second secret ever arrives, the grant is scoped to one parameter ARN rather than
  `/signal/*` — so it needs its own explicit grant rather than silently inheriting this one.
