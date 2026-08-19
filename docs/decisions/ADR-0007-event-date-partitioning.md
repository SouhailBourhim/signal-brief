# ADR-0007 — `silver.articles` partitions on `event_date`, not `published_at`

**Status:** Accepted · **Date:** 2026-08-19

## Context

SPEC §9 originally specified `articles` partitioned by `days(published_at)`. `published_at`
is nullable by design — SPEC §6.2 trusts no timestamp a source claims, and `transform.to_article`
sets it to `None` whenever a source omits a date or the parser can't make sense of what it
sent (`timestamp_flagged=True` records that distrust explicitly, it does not paper over it).

A null value in an Iceberg partition column is a legal, real partition — Iceberg buckets it
under its own null partition — but it can never be *pruned*. Every date-bounded query has to
scan it regardless of the date range asked for, because "unknown date" might mean "matches
your range" for all the engine knows. Two of Phase 2's six sources make this concrete rather
than theoretical:

- `edgar`/`edgar_formd`'s Atom entries carry no `<published>` at all, only `<updated>`
  (`parse/edgar.py`) — not missing by accident, just not part of the format.
- Every RSS 2.0 source's `pubDate` was silently unparseable before `docs/runbooks/
  phase-2.md`'s 2.B date-bug fix, and any future publisher that emits a format
  `feedparse.py` doesn't yet handle regresses to the same failure, silently.

Left on `published_at`, a source degrading in either way doesn't get slower — it gets
*wrong*: its null partition grows without bound, and it is fully scanned by every
date-bounded query indefinitely, which is exactly the §10.3 failure mode Athena's cost
guardrails exist to catch, arrived at from the schema instead of a bad query.

## Decision

`silver.articles` is `PARTITIONED BY (days(event_date))`, where

```
event_date = coalesce(published_at, fetched_at)
```

computed once in `transform.to_article` (never in Spark, so the skeleton's in-process path
and the Iceberg job compute it identically). `fetched_at` is certain — SPEC §6.2 — so
`event_date` is `NOT NULL` by construction: there is no case where both inputs are null.

The trade-off this accepts: a stale-but-successful source (`docs/runbooks/phase-2.md`'s
other open finding) or one with no publish-date field at all lands its articles under the
day they were *fetched*, not the day they claim to have been published. That is a known,
bounded imprecision — bounded by each source's poll cadence, minutes to tens of minutes —
traded against the alternative, which is unbounded: a partition that never stops growing
and is never prunable.

## Consequences

- `SILVER_SCHEMA`/`SILVER_COLUMNS` (`spark/jobs/normalize.py`) and `transform.to_article`'s
  output both carry `event_date` alongside `published_at`; `published_at` itself is
  unchanged and still the field anything caring about *claimed* publication time should read.
- SPEC §9 updated: `articles`' partition spec and column list.
- **Bytes-scanned measurement, done 2026-08-19** (full table in `docs/athena.md`). Same
  question — "articles published on 2026-08-18" (1,106 rows) — filtered on `published_at`
  vs. on `event_date`, both with the same column projection: **73,373 bytes scanned
  filtering on `published_at` (not the partition column) vs. 64,713 filtering on
  `event_date`**, a ~12% reduction on top of the 60% column projection already buys. Real,
  if modest at this table's current size — the bigger, structural point is the one the
  numbers can't show yet: every one of the 1,845 articles in `silver.articles` right now
  has a non-null `published_at`, so filtering on it directly happens to return the same
  1,106 rows *today*. `event_date`'s `coalesce` is the reason that stays true the day a
  source's date field breaks — filtering on `published_at` would silently drop that row
  from every date-bounded query from then on, not just scan it inefficiently.
- `MERGE ... WHEN NOT MATCHED THEN INSERT` only (no UPDATE) means `event_date` is fixed at
  first commit, same as every other `articles` column — consistent with the "a changed
  title is a new `article_id`" rule `normalize_window` already applies.
