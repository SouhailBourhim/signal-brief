# Power BI against the lake

[`docs/athena.md`](athena.md) is the command-line half of this: one question, one answer,
the bytes and dollars printed alongside. This is the other half — a BI client pointed at
the same Athena workgroup, for the questions that are about *change over time* rather than
a single answer. Nothing here is a second copy of the data. Power BI is a reader.

What it is good for: how source health moved over the last month, whether the
accept/reject ratio of the enrichment stage shifted when the prompt version changed, which
publishers are drifting in and out of the brief. What it is not for: alerting. The
CloudWatch alarms and the brief's own footer are the alerting story, and a dashboard
nobody has open at 04:00 does not replace them.

## Before anything else: apply the Terraform

The `gold` database was, until this change, created at runtime by
`ops/athena.py::create_iceberg_table`'s `CREATE SCHEMA IF NOT EXISTS` running as the admin
identity. That left `signal-analyst` — the role you are supposed to query as — with no
Glue permission on `gold` at all, so `gold.brief_items` and the enrichment tables were
unreadable by every identity except the admin one. [`query.tf`](../infra/terraform/main/query.tf)
now declares the database and grants it, along with the Athena metadata actions a driver
calls that `signal athena-query` never does.

Both gaps were confirmed against the live account on 2026-08-26 by assuming the role and
asking, rather than read off the policy:

```
$ aws glue get-database --name gold        # as signal-analyst
AccessDeniedException: ... not authorized to perform: glue:GetDatabase on resource:
  arn:aws:glue:us-east-1:481879233905:database/gold

$ aws glue get-database --name ops         # control — same role, granted database
ops

$ aws athena list-data-catalogs            # what a driver calls on connect
AccessDeniedException: ... not authorized to perform: athena:ListDataCatalogs
```

`gold` already exists in Glue (created 2026-08-23 by the first brief run), so it must be
imported before an apply, or the apply fails with `AlreadyExistsException`:

```bash
terraform -chdir=infra/terraform/main import \
  aws_glue_catalog_database.gold 481879233905:gold
terraform -chdir=infra/terraform/main apply
```

## Where Power BI runs

Power BI Desktop is Windows-only, so it runs on the Windows host, not in WSL2. This is not
a violation of the WSL2 rule in [`CLAUDE.md`](../CLAUDE.md) — that rule is about the
pipeline toolchain (Python, Spark, Terraform, the CLIs), all of which stays where it is.
Power BI is a client that happens to live on the other side of the boundary, and it talks
to AWS over the network like any other client. It never touches the repo.

Install, on Windows:

1. **Power BI Desktop** — Microsoft Store or the standalone installer. They run the same
   connector, but the Store build is MSIX-packaged and reads a virtualized registry, which
   decides where the DSN below has to live. A **User DSN** is read by both, so that is what
   [The DSN](#the-dsn) specifies; a System DSN works only for the standalone build.
2. **Amazon Athena ODBC driver, 2.x, 64-bit** — from AWS's *Connecting to Amazon Athena
   with ODBC* documentation page. Power BI's built-in "Amazon Athena" connector is a thin
   wrapper over this driver; without it the connector is present but cannot connect.

## Credentials: query as the analyst role

Same rule as [`docs/athena.md`](athena.md) — SPEC §17: querying with an admin key undoes
least-privilege even when the person holding it is trustworthy. The wrinkle is that the
driver reads the AWS SDK's credential chain from **Windows'** `%USERPROFILE%\.aws`, not
from WSL2's `~/.aws`, so the profile has to exist on the Windows side.

The obvious way to do that is to copy the admin access key into
`%USERPROFILE%\.aws\credentials`. Don't: it puts a second, long-lived copy of the account's
most privileged credential on a filesystem that is not the one you have been keeping it
on. Point Windows at WSL2's copy instead, with `credential_process`.

`%USERPROFILE%\.aws\config`:

```ini
[profile signal-admin]
region = us-east-1
credential_process = wsl.exe -d Ubuntu-24.04 -e /usr/local/bin/aws configure export-credentials --profile default

[profile signal-analyst]
region = us-east-1
role_arn = arn:aws:iam::481879233905:role/signal-analyst
source_profile = signal-admin
```

The admin key stays in WSL2 where it already lives; Windows shells out for it and the SDK
does the `sts:AssumeRole` hop itself, so what the driver actually holds is a short-lived
`signal-analyst` session. Note that `aws configure export-credentials` prints live
credentials on stdout by design — fine as a `credential_process`, not something to run
into a shared terminal or a log.

**Two things in that command are not decoration, and both were found by running it rather
than by reading the docs.** The obvious form — `wsl.exe -e aws configure
export-credentials --profile default` — fails on this machine, twice over:

    $ wsl.exe -e aws ...
    ERROR: CreateProcessCommon:818: execvpe(aws) failed: No such file or directory

- **`-d Ubuntu-24.04`.** Docker Desktop registers its own WSL distribution and, on this
  host, `wsl.exe -l -v` marks **`docker-desktop`** as the default. So a bare `wsl.exe`
  runs inside Docker's utility VM, which has no `aws` CLI and no `~/.aws` at all. The
  error names the *binary*, never the distro, so the natural next move is to go looking at
  a PATH that was never the problem. Pin the distribution.
- **The absolute path.** `wsl.exe -e` execs the binary directly with no login shell, so
  `/usr/local/bin` is not on `PATH` even in the right distro. `bash -lc "aws ..."` also
  works and is the tempting fix, but it is the worse one: `credential_process` requires
  **clean JSON on stdout**, and a login shell is a thing that can print a banner, an rc
  warning or a version notice into the middle of it. An absolute path cannot.

Verified on 2026-08-26 by running the credential process as Windows invokes it (valid
`Version: 1` JSON returned) and by assuming the role (`sts:AssumeRole` succeeded, session
`assumed-role/signal-analyst/...`). Note there is no `aws.exe` on the Windows side — the
ODBC driver carries its own AWS SDK, which is what reads this file.

That verified the pieces, not the driver. The full chain — MSIX-packaged Power BI spawning
`wsl.exe`, the SDK making the assume-role hop, the driver querying as `signal-analyst` —
was first confirmed end to end on 2026-08-28. Worth stating because the MSIX packaging is
what breaks the System DSN above, so it was a fair question whether it would also block
`credential_process` shelling out to another process. It does not.

Get the role ARN from Terraform rather than the literal above if the account ever changes:

```bash
terraform -chdir=infra/terraform/main output -raw analyst_role_arn
```

## The DSN

Windows → **ODBC Data Sources (64-bit)** → *User DSN* → *Add* → *Amazon Athena ODBC
Driver*:

| Field | Value |
|---|---|
| Data Source Name | `signal-athena` |
| Catalog | `AwsDataCatalog` |
| Database | `ops` |
| Workgroup | `signal` |
| AWS Region | `us-east-1` |
| S3 Output Location | `s3://signal-athena-results-481879233905/` |
| Authentication Type | `IAM Profile` |
| AWS Profile | `signal-analyst` |

**User DSN, not System DSN.** This is the one choice on the form that depends on which
Power BI you installed, and it fails without mentioning DSN scope. The Store build
(`Microsoft.MicrosoftPowerBIDesktop`, MSIX-packaged) runs against a virtualized registry
view and never sees `HKLM\SOFTWARE\ODBC\ODBC.INI`, where System DSNs live. The Driver
Manager finds nothing under the name and returns:

    ODBC: ERROR [IM002] [Microsoft][ODBC Driver Manager]
    Data source name not found and no default driver specified

which reads like a DSN you forgot to create rather than one this process cannot see. A User
DSN lives in `HKCU` and is read by both builds. Found on 2026-08-28 against Store build
2.157.879.0 — the System DSN it could not see was present and fully correct, which is why
the message is worth recognizing rather than re-deriving.

**Authentication Type defaults to `IAM Credentials`, and a DSN saved with that default
looks finished.** In that mode Username and Password *are* the access key id and secret
key, so what gets stored is an auth mode with three empty credential fields. The connector
then reports only:

    We couldn't authenticate with the credentials provided. Please try again.

Choose `IAM Profile` instead. It greys out Username, Password and Session Token — that
greying is the confirmation you are on the right mode — and enables **AWS Profile**, which
is the only field to fill in. Leave *Preferred Role* and *Session Duration* empty:
Preferred Role picks a role out of a SAML assertion in the federated flows and does nothing
here, and the role hop is already declared as `role_arn`/`source_profile` in
`%USERPROFILE%\.aws\config`, so the SDK does the `sts:AssumeRole` itself.

Two of the values above are less meaningful than they look. **Database** is only the
default for an unqualified table name, and every query in [`analytics/powerbi/`](../analytics/powerbi/)
is fully qualified, so it is a fallback that never gets used. **S3 Output Location** is
required by the form but overridden in practice: `enforce_workgroup_configuration = true`
on the `signal` workgroup means the workgroup's own result location wins over whatever a
client asks for. Fill it in correctly anyway — a wrong value here is a confusing thing to
find later precisely because it has no effect.

Then in Power BI Desktop: **Get Data → Amazon Athena → DSN `signal-athena`**, and paste a
query from `analytics/powerbi/` into the advanced/SQL box for each table you want.

The credential prompt that follows opens on its **AAD Authentication** tab. Pick **Use Data
Source Configuration** in the left pane instead. AAD is Microsoft Entra ID sign-in — it
authenticates you to Microsoft and has no bearing on AWS, so the sign-in it offers is one
you should not complete even if it works. On this machine it does not get that far,
failing with a `WAM Error ... ApiContractViolation` out of the Windows Account Manager,
which is easy to read as the reason the connection failed rather than as noise from a step
that was never needed. There is no AWS sign-in to do here: the DSN's profile is the
credential.

## Import, not DirectQuery

Choose **Import**. The argument is usually made about cost, and at this project's current
volume that argument is weak enough that it is worth stating honestly rather than
inflating.

A full refresh of all seven queries, run against the deployed lake on 2026-08-26:

| Query | Rows | Scanned | Cost | Engine time |
|---|---|---|---|---|
| `01_source_health` | 350 | 0.01 MB | $0.000048 | 1,509 ms |
| `02_pipeline_costs` | 57 | 0.00 MB | $0.000048 | 652 ms |
| `03_maintenance_runs` | 92 | 0.01 MB | $0.000048 | 765 ms |
| `04_ingest_outcomes` | 76 | 0.27 MB | $0.000048 | 929 ms |
| `05_brief_items` | 30 | 0.00 MB | $0.000048 | 578 ms |
| `06_score_components` | 180 | 0.00 MB | $0.000048 | 811 ms |
| `07_enrichment` | 4 | 0.00 MB | $0.000048 | 829 ms |
| **Total** | **789** | **0.29 MB** | **$0.000336** | **6.1 s** |

Every one of those is at the 10 MB floor — the whole refresh scans less than a third of
what a single query is billed for. So the honest reasons to prefer Import are not the
dollars:

- **Latency.** 0.6–1.5 seconds of engine time *per query*, before result transfer. In
  DirectQuery every slicer click and cross-filter is a fresh round trip, so a dashboard
  that feels instant on imported data feels broken on this.
- **The cutoff is a failure, not a warning.** `bytes_scanned_cutoff_per_query` is 100 MB
  and `enforce_workgroup_configuration = true` means a client cannot negotiate it. In
  Import mode a query that trips it fails once, at refresh, where you see it. In
  DirectQuery it fails as a visual that renders an error to whoever is looking at the
  report.
- **The data doesn't move that fast.** Pollers run on source-specific schedules and the brief
  is built once a day. Re-querying on every mouse interaction re-reads rows that changed
  hours ago.

Cost does start to matter if DirectQuery is left on a report someone actually uses: 6
visuals × a 50-interaction session is 300 queries, each billed at the 10 MB minimum, or
about $0.014 — still small, but it is now proportional to how much someone clicks rather
than to how much data exists, which is the wrong thing for a bill to track.

Refresh in Desktop is manual. Scheduled refresh from the Power BI *service* would need an
on-premises data gateway installed on this Windows host and left running; given the brief
is a once-a-day artifact, a manual refresh before reading is usually the proportionate
answer.

## Do not point Power BI at the Parquet files

Power BI can read a folder of Parquet, and the bronze bucket is full of Parquet. Reading
it that way will produce a table that looks right and is wrong.

These are Iceberg tables. The Parquet files under `s3://<bronze>/bronze/*.db/` are the
storage layer, not the table: which files are live, which have been superseded by a MERGE,
and which are waiting for the next expiry sweep is a fact held in Iceberg's manifests. A
folder read ignores all of it and returns superseded rows alongside current ones. That
lands hardest on exactly the table the guarantee matters most for — `commit_bronze.py`'s
MERGE on `ingest_id` is what makes replay safe, and it works by writing new files rather
than editing old ones, so a folder read of `bronze.raw_documents` re-inflates every
duplicate that the MERGE exists to collapse.

Go through Athena. It reads the catalog.

## The queries

[`analytics/powerbi/`](../analytics/powerbi/), one file per import target. Each is
projected (never `SELECT *`) and filtered on the partition column where the table has one,
for the reasons measured in [`docs/athena.md`](athena.md#the-measurement-select--vs-projected-vs-partition-pruned).

| File | Table(s) | Grain |
|---|---|---|
| `01_source_health.sql` | `ops.source_health` | source × window |
| `02_pipeline_costs.sql` | `ops.pipeline_costs` | run × dag × task |
| `03_maintenance_runs.sql` | `ops.maintenance_runs` | run × table |
| `04_ingest_outcomes.sql` | `bronze.raw_documents` | day × source × outcome |
| `05_brief_items.sql` | `gold.brief_items` | brief date × rank |
| `06_score_components.sql` | `gold.brief_items` | brief date × rank × component |
| `07_enrichment.sql` | `gold.cluster_enrichment`, `gold.enrichment_rejects` | day × model × status |

Each file's header comment says what the columns mean and, more importantly, what the
NULLs mean — several of these tables use NULL to record "not measured", which is a
different fact from zero, and a Power BI measure that coalesces them silently turns "this
source has no content-freshness signal" into "this source is fresh". Read the header
before building a visual on a column.

Three things that bite when moving these into Power BI:

- **`bronze.raw_documents` is aggregated in Athena, in the query, on purpose.** Every row
  holds the raw fetched bytes in a `payload binary` column. It is the immutable record,
  not a BI source, and importing it row-wise is the exact query the 100 MB cutoff exists
  to stop.
- **`score_components` is a `map<varchar, double>` and ODBC has no map type.**
  `05_brief_items.sql` casts it to a JSON string so the column survives the trip;
  `06_score_components.sql` unnests it into rows, which is what you actually want to chart.
- **The 90-day windows are a starting point.** They keep a first refresh cheap. Widening
  them is fine — at 0.29 MB for the current set there is a lot of headroom — but widen the
  window in the SQL, not by importing everything and filtering in Power BI, or the
  partition pruning stops doing anything.

Row counts above are a snapshot from 2026-08-26; the pollers run continuously, so they
grow. `02_pipeline_costs` returned 56 rows during validation and 57 a minute later.
