# Querying the lake with Athena

SPEC §12's Phase 2 acceptance test: a stranger runs `make up`, answers an ad-hoc
question in Athena, and the bytes scanned and cost of that query are recorded. This is
the walkthrough.

## Setup

Three things from Terraform, once `infra/terraform/main` is applied:

```bash
terraform -chdir=infra/terraform/main output analyst_role_arn
terraform -chdir=infra/terraform/main output athena_workgroup   # "signal"
terraform -chdir=infra/terraform/main output athena_results_bucket
```

Query as the `signal-analyst` role, not your admin credentials — SPEC §17: "I query with
an admin key" undoes least-privilege even when the identity behind it is trustworthy.

```bash
CREDS=$(aws sts assume-role \
  --role-arn "$(terraform -chdir=infra/terraform/main output -raw analyst_role_arn)" \
  --role-session-name athena-query \
  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)
read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN <<< "$CREDS"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
```

Then one command — `DB` defaults to `silver`, so a question against `bronze.raw_documents`
needs it explicitly:

```bash
make athena-query DB=bronze Q="SELECT source_id, count(*) AS n FROM raw_documents GROUP BY source_id"
```

`signal athena-query --sql "..." [--database silver|bronze|ops] [--workgroup signal]` is
the command itself (`ops/athena.py`); the Makefile target is `Q=`/`DB=` sugar over it. It
prints every row, then bytes scanned, dollar cost, and Athena's own engine time —
`make athena-query` *is* the acceptance test, not a description of one.

## Real questions, real answers

Run 2026-08-19, against the deployed lake (account 481879233905). Every number below is
what `signal athena-query` actually printed — nothing here is typed in by hand.

**"How healthy is ingestion — what's the split between successful, empty, and errored
polls per source?"** (the pollers run continuously, so this grows every 15 minutes; the
numbers below are a snapshot, not a fixed fact)

```sql
SELECT source_id, outcome, count(*) AS n FROM raw_documents GROUP BY source_id, outcome
```

| source_id | outcome | n |
|---|---|---|
| edgar | error | 4 |
| edgar | ok | 68 |
| edgar_formd | ok | 14 |
| hackernews | empty | 3 |
| hackernews | ok | 9527 |
| rss_ars | ok | 1 |
| rss_tech | ok | 22 |
| rss_verge | ok | 6 |

**"Why did anything get quarantined instead of becoming an article?"**

```sql
SELECT source_id, parse_error, count(*) AS n FROM parse_rejects GROUP BY source_id, parse_error ORDER BY n DESC
```

| source_id | parse_error | n |
|---|---|---|
| hackernews | missing_title | 272 |

Every rejected row is the same, honest reason: a dead or deleted Hacker News item with
no title — SPEC §6.2's quarantine, not a bug (`tests/test_parse.py::
test_hackernews_dead_story_has_no_title_and_is_quarantined_not_dropped` covers exactly
this case).

**"Which publishers contributed the most on August 18th?"**

```sql
SELECT publisher_domain, count(*) AS n FROM articles
WHERE event_date >= TIMESTAMP '2026-08-18 00:00:00' AND event_date < TIMESTAMP '2026-08-19 00:00:00'
GROUP BY publisher_domain ORDER BY n DESC LIMIT 5
```

| publisher_domain | n |
|---|---|
| sec.gov | 810 |
| techcrunch.com | 22 |
| github.com | 18 |
| arstechnica.com | 13 |
| news.ycombinator.com | 11 |

SEC filings dominate by volume — expected, since `edgar`/`edgar_formd` poll every 15
minutes against a feed that reports every filing, while the RSS sources report a
curated handful of stories per poll.

## The measurement: `SELECT *` vs. projected vs. partition-pruned

This is ADR-0007's own justification, measured rather than asserted. Same logical
question — "articles published on 2026-08-18" — three ways, against `silver.articles`
(1,106 rows on that day):

| # | Query | Filters on | Bytes scanned | Cost | Engine time |
|---|---|---|---|---|---|
| 1 | `SELECT *` | `published_at` (not the partition column) | 184,259 | $0.0000477 (floor) | 1,592 ms |
| 2 | `SELECT article_id, publisher_domain, title` | `published_at` | 73,373 | $0.0000477 (floor) | 568 ms |
| 3 | `SELECT article_id, publisher_domain, title` | `event_date` (the partition column) | 64,713 | $0.0000477 (floor) | 1,511 ms |

Read top to bottom: **column projection** (#1 → #2) is the bigger win at this table's
current size — dropping `body_text` and the other unselected columns cuts the scan by
60%, because Parquet stores columns separately and Athena only reads what a query
actually asks for. **Partition pruning** (#2 → #3) adds another ~12% on top by filtering
on `event_date` — the column `silver.articles` is actually `PARTITIONED BY (days(...))`
— instead of `published_at`, which carries the same information for a human reading the
row but tells Iceberg nothing about which files to skip.

Two things worth being honest about rather than glossing over:

- **The dollar cost is identical across all three.** Every one of these scans is far
  below Athena's 10 MB per-query minimum, so `athena_cost_usd`'s floor (SPEC §17) makes
  all three round to the same figure. At this table's current size (a few thousand
  rows), bytes scanned is the metric that actually moves; cost only starts to
  differentiate once a query's real scan exceeds 10 MB. Both are printed, for exactly
  this reason — reporting only the (identical, currently uninformative) dollar figure
  would hide the real, measured difference.
- **Filtering on `published_at` instead of `event_date` is a correctness risk, not only
  a performance one, and today's data doesn't get to demonstrate it.** All 1,845 articles
  currently in `silver.articles` have a non-null `published_at` — every one of the six
  live sources currently emits a usable date. `event_date`'s `coalesce(published_at,
  fetched_at)` (ADR-0007) exists for the source that eventually doesn't: filtering on
  `published_at` directly would silently exclude that row from every date-bounded query
  forever, not just scan it inefficiently. The row counts above happen to match (1,106 =
  1,106) because the case ADR-0007 defends against hasn't occurred yet — which is the
  right time to have already built the defense.

## A BI client instead

The same workgroup, the same `signal-analyst` role, read from Power BI — for the questions
that are about change over time rather than a single answer. Setup, the query set, and the
measured case for Import over DirectQuery: [`docs/powerbi.md`](powerbi.md). It reads these
tables and nothing else depends on it (ADR-0012).

## Guardrails

`enforce_workgroup_configuration = true` on the `signal` workgroup means its own
settings — including `bytes_scanned_cutoff_per_query` (100 MB) — win over whatever a
client requests. A query that trips it is a query to rewrite, not a limit to raise
(SPEC §10.3).
