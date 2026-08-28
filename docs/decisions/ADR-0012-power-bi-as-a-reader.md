# ADR-0012 — Power BI is a reader, not the monitoring layer

**Status:** Accepted · **Date:** 2026-08-26

## Context

`query.tf` describes the `ops` database as "pipeline health and cost, **as data rather
than a dashboard**", and that phrasing is load-bearing. It is why `ops.source_health` is a
MERGE-keyed table rather than a metric emitted to a graphing service: the brief's footer,
Airflow's alerting, and "was the pipeline healthy last Tuesday?" all read the same rows,
and a question about last Tuesday is answerable at all only because the answer was stored
rather than drawn.

Adding a BI tool sits awkwardly next to that sentence, so the tension is worth resolving
explicitly rather than leaving the next person to wonder whether the dashboard is now the
source of truth.

There is also a real gap the request exposed. `signal athena-query` is handed a database
name and never browses, so nothing had ever exercised the catalog-discovery path. A driver
does browse — and `signal-analyst` could not read `gold` at all, because the database was
conjured at runtime by `CREATE SCHEMA IF NOT EXISTS` as the admin identity and the analyst
policy grants databases by enumeration. The gold marts had been readable by exactly one
identity, the admin one, since 2026-08-23. This is the same class of defect as ADR-0005's
silent `AccessDenied`: a permission that was never wrong, only never present.

## Decision

Power BI connects to the existing Athena workgroup as `signal-analyst`, in **Import**
mode, over the query set in `analytics/powerbi/`. It reads. It is not written to, not
scheduled, and not depended on by anything in the pipeline.

Alerting stays where it is: CloudWatch alarms on the AWS half, the brief's own footer for
the daily read, `ops.source_health` as the queryable record underneath both. If a fact
matters enough to alarm on, it belongs in a table and an alarm, not in a report.

`gold` becomes a declared `aws_glue_catalog_database` and is added to the analyst policy,
alongside the Athena metadata actions (`ListDataCatalogs`, `GetDataCatalog`,
`ListDatabases`, `ListTableMetadata`, and their `Get` counterparts) that any driver calls
on connect.

## Consequences

- The dashboard is downstream of the tables and can be deleted without losing a fact. That
  is the property that makes it safe to add; if a future visual computes something not
  recoverable from a query, that computation is in the wrong place.
- Import mode means the report is as old as its last refresh, and Desktop refreshes
  manually. Accepted deliberately — see the measured latency table in `docs/powerbi.md`.
  Scheduled refresh would require an on-premises gateway running on the Windows host, which
  is a service to operate in exchange for freshening a once-a-day artifact.
- `athena:ListDataCatalogs` is granted on `*` because it has no resource type — it is the
  call that discovers catalog ARNs. Written as an explicit statement with a comment rather
  than folded into the scoped one, so that the single unscoped action in this policy is
  visible instead of buried.
- Power BI Desktop runs on the Windows host, outside WSL2. This does not weaken ADR-0002:
  nothing in the pipeline toolchain moved, and the client reaches AWS over the network like
  any other. It does mean AWS credentials must resolve on the Windows side, handled with
  `credential_process` shelling into WSL2 rather than by copying the admin key across.
- The existing `gold` database must be `terraform import`ed before the next apply, or the
  apply fails with `AlreadyExistsException`. The command is in `docs/powerbi.md`.
