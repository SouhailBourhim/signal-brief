# Phase 4B runbook — enrich + macro

> **Amended 2026-08-24 — the brief now sends at 16:00, not 07:00.** The times recorded
> below are what the schedule was at the time and are left as written. The send moved
> because the host sleeps through the small hours: on 2026-08-24 the scheduler logged
> nothing between 21:00 and 12:58 UTC, then resumed mid-stride and fired the whole chain
> at once, so the brief landed at 13:59. The containers never died — they were frozen
> with the host, which still reported them `Up`. See `airflow/dags/brief_dag.py`.

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

## What broke on first real use

### `make clean` silently stopped ingestion for ten hours

Found 2026-08-23 while checking a failing DAG, not by any alarm — which is the part that
matters.

**Symptom.** `ingest_monitor` succeeded at 14:05 UTC on 2026-08-22 and failed on every run
after it. `commit_staged` died in ~4 seconds with:

    FileNotFoundError: [Errno 2] No such file or directory: '/opt/signal/.cache/staging'

from `staging.sync_staging`, inside a `Path.mkdir(parents=True)` that was recursing all the
way up and still failing.

**Cause.** `make clean` deletes `.cache`, and `.cache` is bind-mounted into all three Airflow
containers (`docker-compose.yml`). Deleting the host directory while the containers are up
breaks the mount **at the inode level**: the container's view survives as a directory with
**link count 0**, and every `mkdir` inside it fails with ENOENT.

    # inside the container, after `make clean` on the host
    drwxr-xr-x 0 default 1000 0 Aug 22 15:50 /opt/signal/.cache
    #          ^ link count 0 — the inode is gone

Recreating the directory on the host does **not** fix a running container. The mount has to be
re-established, which means recreating the containers.

**The Makefile already knew.** The `clean` target carried a comment saying exactly this,
including the recovery command, from 2.E. A comment is not a guard, and the failure it
describes is invisible: nothing alerts on a local Airflow DAG failing, so ten consecutive
failures produced no signal anywhere.

**Fixed properly rather than re-documented.** `make clean` now checks whether the Airflow
containers are running and keeps the bind-mounted paths if they are, saying why;
`make clean-mounted` does the destructive version and recreates the containers so the mounts
survive.

**The first version of that guard was wrong in the same way, within the hour.** It protected
`.cache` and nothing else — but `docker-compose.yml` also mounts `./data` and `./out`, and
running `make clean` to test the guard promptly broke both. `/opt/signal/out` went to link
count 0, which is where the 07:00 brief writes its HTML: the send would have failed on the
first morning of 4A's acceptance, from a fix intended to prevent exactly that.

Caught by checking the container's view rather than trusting the guard, and it is why
`MOUNTED_PATHS` is now a variable listed beside the `volumes:` block it has to track, instead
of a path inlined in a recipe. **A guard derived from an incident is only as good as its
inventory**, and the inventory is the part that silently goes stale.

**What it cost, and what it did not.** Ingestion itself never stopped — the pollers are Lambdas
on EventBridge and kept staging to S3 throughout. What stopped was the *commit* into
`bronze.raw_documents`, so 1,738 staged objects (22.3 MB) accumulated and were merged in one
catch-up run. **No data was lost**, which is Phase 1's replay guarantee doing exactly its job:
staging is a queue, the MERGE is on `ingest_id`, and re-reading an already-committed interval
inserts nothing. The only real cost was re-downloading 22 MB of staged objects (~$0.002),
because `make clean` had also wiped the local read-once cache that normally makes a re-sync
free.

### The same command left a directory nobody could delete

`airflow/dags/__pycache__` was owned by uid **50000** — the airflow image's default user —
with group root and mode 775. The host user (uid 1000) could neither write to it nor delete
what was inside, so `make clean` exited non-zero on every single run. 4A's runbook noted this
as a papercut and left it.

The cause is that the DAGs folder is a bind mount, so bytecode written by the container lands
in the repo owned by a uid the host does not control. Fixed at the source:
`PYTHONDONTWRITEBYTECODE: "1"` in the shared Airflow environment, so the directory is never
created. The Lambda package has set this for its own reasons since Phase 1.

### Four DAGs had never run, because Airflow pauses new DAGs by default

The larger finding, and it is not a 4B one — it is about 4A.

| DAG | Added | State | Runs |
|---|---|---|---|
| `brief` | 4A | **paused** | **none** |
| `market` | 4A | **paused** | **none** |
| `maintenance` | 4A | **paused** | **none** |
| `resolve` | 3.C | **paused** | **none** |
| `ingest_monitor`, `process`, `cluster` | 1-3 | active | running |

Airflow pauses a newly-discovered DAG unless `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` says
otherwise, and this stack does not set it. So **4A's local half was built, merged, deployed and
never executed.** The AWS half — nine Lambdas on EventBridge — has been running the whole time,
which is exactly why nothing looked wrong: bronze kept filling, the brief kept being
buildable by hand, and the one thing SPEC §12's 4A acceptance actually turns on (a mail at
07:00) had never happened once.

**This is why the acceptance is a behavioural test rather than a green build.** Every unit test
passes, `make skeleton` passes, the Terraform applied cleanly, and the phase was still not
running. Only asking "has it sent a brief yet" finds that.

### Registering source #9 turned the ingestion DAG red, correctly

Caught minutes after fixing the mount, by re-running the DAG rather than assuming the fix was
the whole story.

`commit_staged` succeeded — 1,738 staged objects merged, the ten-hour gap closed. Then
`raise_on_degraded` failed with exactly one degraded source:

    degraded sources: [{'docs': 0, 'status': 'never_succeeded', 'source_id': 'macro', ...}]

**The monitor was right.** `DEPLOYED_SOURCE_IDS` is derived from `config.SOURCES`, so adding
`macro` immediately made `ingest_monitor` assess a source whose Lambda does not exist yet —
`terraform apply` has not been run for it. A configured source producing nothing *is* the
failure `never_succeeded` exists to catch.

But an hourly red run for a known, deliberate, not-yet-deployed state is the
alarm-nobody-reads failure §11 keeps warning about, and worse: it would mask a real outage in
any of the other eight, since `raise_on_degraded` fails on the aggregate.

So there is now a `NOT_YET_DEPLOYED` set in `config.py` for the window between the commit that
adds a source and the apply that creates it. `fake` was already excluded by name for precisely
this reason — "assessing it would report a permanent outage for something that was never
running" — so this generalizes an argument the config already made.

Two tests keep it from rotting, in both directions:

- `test_a_pending_source_is_really_pending` — every entry must be a real source with a real
  Terraform entry, or the set is a typo silently excluding something from monitoring.
- `test_nothing_deployed_is_silently_unmonitored` — every source with a Terraform entry is
  either monitored or explicitly pending. Anything else is a deployed source nothing watches.

**Remove `macro` from `NOT_YET_DEPLOYED` in the same change that applies its Terraform.**

### `gold.brief_items` had never existed, and 4A's acceptance turned on it

The sharpest finding of the session, and it is 4A's, not 4B's. Found by running
`signal brief` against the real account for the first time — which nothing had done, because
the `brief` DAG was paused.

`information_schema` said it plainly: **the `gold` schema held zero tables.** The feedback
loop SPEC §12's 4A acceptance is built on — "you read it three mornings running and the
feedback loop records your marks" — had never once worked.

The DDL was wrong in three independent ways, each of which Athena rejects with a message that
names neither the column nor the table:

| Wrong | Right | Athena's complaint |
|---|---|---|
| `WITH (table_type = 'ICEBERG', format = 'PARQUET')` | `LOCATION '...' TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet')` | `no viable alternative at input` |
| `map(varchar, double)` | `map<string, double>` | `no viable alternative at input` |
| `rank integer`, `cluster_id varchar` | `rank int`, `cluster_id string` | `type expected at the position 0 of 'integer'` |

All three are valid **Trino** — in a `SELECT` or an `INSERT`. Athena's `CREATE TABLE` takes
Hive-style DDL, and `LOCATION` is required for Iceberg rather than defaulting from the Glue
database. The two dialects meet inside one module, which is why `MAP(ARRAY[...], ARRAY[...])`
in the same file is correct and `map(varchar, double)` twelve lines above it is not.

**Why every test passed.** `tests/test_brief_items.py` injects a fake Athena client that
records SQL and never parses it — which is exactly what makes it good for asserting on the
DELETE's WHERE clause, and exactly why it cannot see a syntax error. One assertion was
actively pinning the bug: it checked for `table_type = 'ICEBERG'`, the unquoted form only the
broken `WITH` syntax produces.

**Now fixed, verified against the real account, and guarded.** `create_iceberg_table` in
`ops/athena.py` is the single place both gold writers go through, and it carries the dialect
notes. Three static tests close the half of the gap a fake client can: every declared type
must be in Athena's documented Iceberg type set, the statement must use
`LOCATION`/`TBLPROPERTIES` rather than `WITH`, and the table must land at
`<warehouse>/<namespace>.db/<table>` where Spark already puts everything else.

The brief now builds end to end against the real lake — 2,979 clusters, 1,031 company links,
**8 sources 0 not ok**, both 4B tables degrading loudly rather than failing — and writes ten
rows. `signal feedback --list` and `signal feedback <id> up` both work. **That is 4A's
acceptance mechanism functioning for the first time.**

**The lesson is the one this project keeps relearning, in its sharpest form yet.** Every unit
test passed, `make eval` passed, `make skeleton` passed, CI was green, the Terraform applied,
and the PR merged — against a table that did not exist, through a DAG that had never run, for
a phase whose acceptance depended on both. Only running it found any of it.

### Athena and Spark disagree about one table property, fatally

Found immediately after the above, by asking whether the 02:00 maintenance sweep would
survive a table Athena had created — the first one it would ever reach, since
`gold.brief_items` had not existed until that day.

Spark **reads** it fine (10 rows). `rewrite_data_files` then dies:

    IllegalArgumentException: Property 'write.object-storage.path' has been deprecated
    and will be removed in 2.0.0, use 'write.data.path' instead

Athena stamps every Iceberg table it creates with `write.object-storage.enabled=true` and
`write.object-storage.path=...`. The Iceberg runtime this project pins (ADR-0006) has
deprecated the second name and **raises** on it rather than warning. So a nightly sweep that
includes any Athena-created table goes red forever — and `MAINTAINED_TABLES` gained three of
them in 4B plus 4A's `gold.brief_items`.

**It cannot be fixed where the table is created.** Verified against the deployed account, both
directions:

    CREATE TABLE ... TBLPROPERTIES ('write.object-storage.enabled'='false')
      -> Unsupported table property key: write.object-storage.enabled
    ALTER TABLE ... UNSET TBLPROPERTIES ('write.object-storage.enabled')
      -> Table property write.object-storage.enabled is not supported by Athena

Athena will neither set nor unset the property it sets itself. So the side that trips on it is
the side that clears it: `maintain_table` drops both properties before compacting, and only
when they are present, so Spark-created tables take no extra metadata commit.

Dropping it costs nothing measurable. Object-storage layout spreads S3 keys across prefixes to
avoid request-rate hotspots on very large tables; these are gold marts with tens of rows. What
it buys is a sweep that runs.

Verified on the real table after the fix: `gold.brief_items` compacted **3 files → 2**, error
`None`, with `remove_orphan_files` skipped for 4A's documented S3 reason.

### `resolve` could never have found its dictionary

Found by pre-flighting a DAG *before* triggering it, having learned the lesson twice already
in one session.

`entities/dictionary.py::DEFAULT_PATH` was `Path("warehouse/entities/dictionary.json.gz")` —
**relative**, so it resolves against the process's cwd. Airflow tasks run with
`cwd=/opt/airflow`, so inside the containers it pointed at `/opt/airflow/warehouse/...`. And
`./warehouse` was not in `docker-compose.yml`'s `volumes:` at all, so no path would have
worked.

`resolve_dag` would have raised `FileNotFoundError` on its very first run. It never did,
because it had been paused since 3.C — and Phase 3 ran the resolver locally from the repo
root, where the relative path is correct.

**`docker-compose.yml` already knew about this class of bug.** It sets `SIGNAL_DATA_ROOT`,
`SIGNAL_OUT_ROOT` and `SIGNAL_CACHE_ROOT` absolutely, under a comment that says exactly why:
*"Airflow tasks run with cwd=/opt/airflow and the defaults in config.py are relative."* The
dictionary path was the one relative path with no setting behind it to override, so it was
invisible to that fix.

Now `Settings.entity_dictionary_path`, overridden in compose like its three siblings, with
`./warehouse` mounted read-only. Verified inside the container: the path resolves absolutely
and the snapshot loads, **11,835 entities**.

`tests/test_compose_paths.py` pins the whole class rather than this instance: every relative
path setting must be overridden absolutely in compose, the dictionary must exist, and
`make clean`'s `MOUNTED_PATHS` must cover every bind-mounted directory it could delete. It
asserts the defaults are still relative too, so it cannot quietly become vacuous.

### The stage enriched 2,979 clusters instead of 40, and said nothing while doing it

The first real run. Reported as *"takes too long and shows nothing"*, and both halves were
real defects — neither in the setup, which was correct.

**`rank` marks the cut; it does not apply it.** `ranker.rank` returns **every** scored cluster
with only the top `limit` flagged `included`, and `run` passed `window.clusters` straight
through. So a 2,979-cluster window sent 2,979 heads to the GPU rather than 40: **~70 minutes,
spent mostly on exactly the routine SEC filings ADR-0011 exists to keep out.** The argument was
written in this module's own docstring and the code did the opposite of it.

Nothing would have caught it. Every counter in `EnrichmentRun` was internally consistent, every
row it would have written was individually correct, and the only thing wrong was *how many*.
`test_run_enriches_only_the_ranked_cut_not_the_whole_window` pins it now.

**And it printed nothing for the entire batch.** `run` had no progress output and `cli_enrich`
only reported after everything finished. This stage is legitimately slow — ADR-0003's ~22.5 s
model load, then ~1.5 s per head — and a slow stage that is silent is indistinguishable from a
hung one. That is what made a 70-minute bug look like a hang instead of a runaway. Fixed with a
`progress` callback, defaulting to a no-op so the DAG and tests stay quiet.

**A cached run was paying for a model load to infer nothing.** The structured-output probe is a
real generation, so it was pulling 5.3 GB into VRAM before discovering there was nothing to do —
16.5 s on the steady-state path the 06:15 DAG hits every morning after the first. A cache that
still pays for a model load has given back most of what it saves. The probe is lazy now,
resolved on the first head that actually needs the model.

### What the first real batch measured

| | |
|---|---|
| Heads | 40 |
| Wall clock | **80.2 s** (~1.5 s each after load) |
| Schema failures | **0 of 40** |
| Second run | **100% cache, 0 inferred, 0 rows written, no model call** |

ADR-0003 predicted ~1 minute for 40 heads and measured ~1.0 s per head on much shorter prompts.
At 4,126 prompt characters the real figure is ~1.5 s, so the capacity bound
(`CAPACITY_SECONDS_PER_HEAD = 8.0`) has roughly 5x headroom — comfortable, and now backed by a
measurement rather than an extrapolation.

**Zero schema failures on the first 40 is worth not over-reading.** Ollama's schema-constrained
decoding is doing most of that work; it is a statement about constrained decoding, not about the
model's judgement, and `gold.enrichment_rejects` exists for the cases where it stops holding.

The topic enum survived contact with real data. All ten values were reachable, the distribution
is plausible (`business-corporate` 8, `software-engineering` 8, `other` 6, `ai-ml` 5), and the
one SEC filing that reached the top 40 was labeled `sec-filing` with `filing_type: "144"` — the
corpus-fitted decision validated by output rather than by argument. **`other` at 6 of 40 is the
number to watch**: an escape hatch carrying 15% is doing real work, but if it climbs the enum is
missing a category.

### One quality finding to carry into the labeling

    "company": "Tesla, Uber, and Waymo"

`Extraction.company` is documented as "the primary company named" and is a single value. The
model comma-joined three of them rather than picking one or abstaining. The schema accepts it
because it is a valid string, and field-level exact match will score it wrong — correctly — once
the labeled set exists. Worth deciding deliberately when labeling: either the prompt says "the
single primary company, or null if several are equally central", or the field becomes a list.
Recorded rather than fixed, because changing the prompt bumps `PROMPT_VERSION` and invalidates
every cached enrichment, which is not a thing to do casually at 01:00.

### `.env` reached into the containers and pointed them at themselves

Surfaced by unpausing the `enrich` DAG and asking whether it could actually reach Ollama —
which, after the day this had been, seemed worth checking before 06:15 rather than after.

`docker-compose.yml` had:

    SIGNAL_OLLAMA_URL: ${SIGNAL_OLLAMA_URL:-http://host.docker.internal:11434}

The default is right. **Compose never used it.** `${VAR}` is expanded from the project `.env`
*before* the container starts, and `.env` carries `SIGNAL_OLLAMA_URL=http://localhost:11434` —
correct for `uv run signal enrich`, and inside a container an address that resolves to the
container itself. Every enrichment task would have failed with `OllamaUnavailable` against a
URL nobody chose.

Verified from inside the scheduler:

| Address | Result |
|---|---|
| `localhost` (what `.env` supplied) | connection refused |
| `host.docker.internal` (the ignored default) | **200** |
| `gateway.docker.internal`, `172.18.240.1` | refused |

**This is the third instance of one pattern in one day**, and the pattern is worth naming:
*a setting that is correct on the host and wrong in a container, with nothing to say so.*
`SIGNAL_DATA_ROOT`, `SIGNAL_OUT_ROOT` and `SIGNAL_CACHE_ROOT` were hardcoded absolute long ago
for precisely this reason — the comment above them says so. The entity dictionary had no
setting at all and defaulted to a relative path. And this one *had* the right default and was
overridden by the mechanism meant to make it configurable.

Fixed by moving the override to a differently-named variable (`COMPOSE_OLLAMA_URL`), so a
local `.env` cannot collide with it. `test_no_host_specific_setting_is_interpolated_from_the_local_env`
pins the class: any `SIGNAL_*` setting that names a *location* must not be self-interpolated in
compose. Settings that name a *resource* — `SIGNAL_BRONZE_BUCKET`, `AWS_REGION` — are excluded
deliberately, because those mean the same thing on both sides and inheriting them is the point.

### The prompt version was stamped `v0` on output produced by the `v1` prompt

The worst of the day's findings, because it was silent and it corrupted the record rather
than stopping anything.

`Settings.prompt_version` defaulted to `"v0"` — a Phase 0 placeholder, from before any prompt
existed. `prompt.PROMPT_VERSION` said `"v1"`. **The cache key and every stored row took the
setting.** So the first 40 real enrichments were written stamped `v0`, produced by the v1
prompt, under an `input_hash` computed with `v0`.

That is not cosmetic. §7.3's entire governance claim is *accuracy tracked per model and prompt
version*, and a stamp that does not name the prompt that produced the row makes every model or
prompt comparison meaningless. Worse for the cache: bump the prompt to v2 and those rows keep
their v0 key, so nothing collides and nothing is invalidated — the mechanism designed to make
a prompt change visible would have let it pass unnoticed.

**Fixed by removing `prompt_version` from `Settings` entirely.** The prompt module owns the
version because the prompt is the thing that changes, and it is deliberately *not* a setting:
an operator able to override it could serve output under a stamp that never described it,
which is exactly the lie the three-part key exists to prevent.

The 40 mislabeled rows were deleted rather than re-stamped. They were keyed under a version
that never described them, so they were unreachable under the corrected key anyway — dead rows
asserting something false. 80 seconds of GPU to regenerate.

Found only because the first `enrich` DAG run failed for an unrelated reason and the container's
settings were printed side by side with the host's.

### The DAG failed on its first run, correctly, against a config gap

`RuntimeError: refusing to enrich with an unpinned model digest (ADR-0003)`.

The guard added hours earlier, doing precisely its job. `SIGNAL_OLLAMA_MODEL_DIGEST` lives in
`.env`, `.env` is not mounted into the containers, and `docker-compose.yml` did not pass it
through — so the container saw `UNPINNED` while the identical code from a shell had the pin.

Note the asymmetry with the URL finding directly above: the digest and model tag **should** be
inherited from `.env`, because they name a *resource* that means the same thing on both sides.
The URL must **not** be, because it names a *location* that does not. Both are now explicit,
and `test_no_host_specific_setting_is_interpolated_from_the_local_env` encodes which is which.

Worth saying plainly: this failure was the good kind. It refused to write rows keyed on a
string that does not name a model, which is what the guard was for.

### `DFF` and `DGS10` failed every real invocation: FRED's real cap is vintage dates, not bytes

Found by invoking the deployed Lambda for real, immediately after confirming the key and
`terraform apply` — checking the thing rather than the proxy for it.

    {"source_id": "macro", "documents": 6, "outcomes": {"ok": 4, "error": 2}}

Both failures were HTTP 400 from FRED itself:

    Bad Request.  There are 2885 vintage dates in the specified real-time period:
    2015-01-01 to 9999-12-31.  This exceeds the maximum number of vintage dates
    allowed for this file type (2000).

**The constraint this module's own docstring cited was the wrong one.** It reasoned about a
100,000-*observation* ceiling and sized `REALTIME_START` against that. The limit that actually
bites is a **2,000-*vintage-date*** cap, and it has nothing to do with size: a monthly series
like `PAYEMS` accumulates one or two vintages per period and is nowhere near it after a decade,
while a **daily** series — `DFF`, the effective fed funds rate; `DGS10`, the 10-year constant
maturity — racks a new vintage roughly every business day whether or not the value was ever
"revised" in the sense §8 cares about. Only 2 of 6 watchlist series are daily, which is why 4
of 6 succeeding looked like partial success rather than a systemic bound.

**Measured the actual boundary before picking a fix**, by bisection against the live API:

| Years back | `DFF` | `DGS10` |
|---:|---|---|
| 11 (the shipped bound) | 2730 vintages — FAILS | 2719 — FAILS |
| 9 | 2239 — FAILS | 2228 — FAILS |
| 8 | succeeds | succeeds |
| 5 | succeeds, ~1245 vintages | succeeds |

Eight years technically clears the cap — at **99.6% of it**. That is not a bound with margin,
it is a boundary hugged, and a fixed calendar anchor only ages toward it: the same 11-year
window that failed today was presumably fine when this was designed against "2015 to some
earlier now." A bound stated as a fixed date decays; the fix is the one this project has
already used once, for backfill horizons — measure **backward from now**, not from a fixed
point.

`REALTIME_START` is now `VINTAGE_WINDOW_YEARS = 5`, resolved at poll time. Threaded through
`_fetch_series` and `_document` rather than left as a module constant, so the record stored in
bronze — `source_url` — names the window actually requested rather than a stale one (SPEC
§6.1: the record must not describe a fetch that did not happen). 5 years holds `DFF` to ~60%
of the cap, comfortable margin, and is drastically more history than §8's own worked payoff
needs ("payrolls revised down 46k across the prior two months" is about recent revisions).

**Rebuilt, re-planned, not yet re-deployed as of this writing.** `terraform plan` after
`make lambda-package` shows **9 to change, 0 to destroy** — every poller's code hash moves,
because all nine ship from one zip; nothing else drifts. `NOT_YET_DEPLOYED` keeps `"macro"`
until the live Lambda's `CodeSha256` actually matches the fixed build, confirmed by direct
invocation, not by `terraform apply` having exited zero — the same distinction the September
2026 finding above draws between "applied" and "actually reachable."

### `macro` deployed for real, verified, and unmarked as pending

Confirmed the deploy the right way — by the live `CodeSha256` matching the fixed build and a
real invocation, not by `terraform apply` exiting zero.

    deployed CodeSha256: Dck9nSZR7lBAY5VQp2YCX9YJDoRj9w87npJQj8sLJhU=
    fixed build hash   : Dck9nSZR7lBAY5VQp2YCX9YJDoRj9w87npJQj8sLJhU=   — match

    {"source_id": "macro", "documents": 6, "outcomes": {"ok": 6}}

All six series, all `ok`, all sharing the corrected 5-year window — `DFF` and `DGS10`
included, with real observation counts (26,349 and 16,865 respectively).

`macro` is out of `NOT_YET_DEPLOYED`. `DEPLOYED_SOURCE_IDS` is 9; `ingest_monitor` now
assesses it for real every hour, the way it has assessed the other eight since Phase 1-4A.

### `test_run_query_times_out_rather_than_polling_forever` was a wall-clock race

Found by an external review run against this runbook's own "everything is green" claim, not
by a failure in this repo's own CI — worth recording for that reason as much as for the bug.

The test gave the fake Athena client a finite `polls_before_terminal=10_000` and a 50 ms
timeout budget, with `poll_interval_seconds=0`. The intent was "assert `TimeoutError` fires
rather than polling forever." What it actually asserted was **whichever finishes first: 10,000
tight-loop Python calls, or 50 ms of wall-clock time** — a race between the fake's call count
and real elapsed time, decided by how fast the machine happens to be. On this machine, right
now, it passed every time it was run. That is not the same claim as "this test is correct" —
a faster interpreter, a loaded CI runner, or a slower one could flip the outcome either way,
and a test whose result depends on the speed of the machine running it is not testing the
thing its name says it tests.

Fixed by removing the race rather than widening the budget to hide it: `polls_before_terminal`
now accepts `float("inf")`, so the fake can never reach a terminal state and `TimeoutError` is
the only way the loop can end, on any machine. Verified deterministic across five repeated
runs rather than trusting one green result — the same standard this runbook's other findings
were held to.

**The claim below that `make test` was green is corrected, not just re-asserted.** It was true
of the specific run that produced it, and it was still an unreliable thing to have claimed,
because this test could have failed that same run on different hardware. Re-verified clean
after the fix.

## Acceptance

SPEC §12's 4B gate, item by item. **Not met** — two external gates and a calendar one.

| Asked for | Where it is | State |
|---|---|---|
| Ollama stage with content-hash cache | `enrich/` — client, prompt, schema, store, run | Built |
| Pydantic validation, quarantine | `enrich/schema.py`, `gold.enrichment_rejects` | Built |
| Eval harness, 100 examples | `score_enrichment`, `sample_enrichment.py`, `enrichment_predict.py` | Harness built; **examples blocked on Ollama** |
| ALFRED bitemporal macro store | `sources/macro.py`, `spark/jobs/macro.py`, `gold.macro_observations` | Built |
| Revisions in the brief | `read_macro_revisions` + the template's revision block | Built |
| 30-day reproducibility backfill | `ops/reproduce.py`, `signal reproduce` | Harness built; **window closes 2026-09-17** |

### What is left, and who clears it

| Blocked on | Command |
|---|---|
| **Ollama running on the host** — then verify the digest against ADR-0003 and pin it | `signal enrich --check-model` |
| **A free FRED key** in the SSM parameter Terraform creates | `aws ssm put-parameter --name /signal/fred-api-key --type SecureString --value <key> --overwrite` |
| **`terraform apply`** for `macro.tf` and source #9 | `terraform -chdir=infra/terraform/main apply` |
| **30 days of bronze** — 2026-09-17 at the earliest | `signal reproduce --days 30` |

Neither gate blocks construction: every module here is tested against `respx` and `moto`, and
`make test`, `make lint`, `make eval`, `make skeleton`, `make lambda-package` and
`make tf-validate` are all green.

The rehearsal on five days is the next thing to run once the first two clear.

## Then

**Phase 5**, per SPEC §12: dbt migration of silver→gold, and Kafka **if and only if** §14's
criteria are met. Its acceptance is 14 consecutive daily briefs — which cannot start counting
until 4A's does, and 4A's did not start until 2026-08-23, for the reason recorded above.

4B carries forward everything it deliberately declined, plus what it found:

| Item | Recorded in | Gates |
|---|---|---|
| **ADR-0009's embedding branch behind `dedup.decide`** | ADR-0009, 4B decisions | Dedup recall's 0.500 ceiling |
| **§7.4's novelty component** — still absent from `WEIGHTS` | 4A.H, ADR-0009 | The ranker is five-sixths of its spec |
| **The resolver's `?itemDescription` fix and wider candidate set** | ADR-0009 | Entity recall |
| **The 100 enrichment examples and the `[enrichment]` floors** | 4B.G | `make eval` gating enrichment at all |
| **Nothing alerts on a local DAG failing** | 4B "what broke" | Ten hours of dead ingestion produced no signal; SPEC §11's monitoring covers the AWS half only |

That last one is new and is the sharpest of them. §11's whole argument is that silence is the
failure mode, and the monitoring built for it watches Lambdas — which were fine. The half that
broke was the local half, and it broke silently for ten hours behind a green AWS console.
