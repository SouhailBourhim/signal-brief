# ADR-0019 — Poller alarms are fleet-wide, because per-source ones bought attribution nobody read

**Status:** Accepted · **Date:** 2026-08-30 · **Narrows the alarm set introduced in Phase 1**

## Context

`monitoring.tf` created three alarms per source with `for_each = var.sources` — `errors`,
`not-running`, `throttled`. That was three alarms when the map had one source. By 2026-08-30
the map has nine, so it was 27, plus the two local-half alarms from Phase 5: **29 alarms.**

CloudWatch's free tier is 10 alarms per month, always-free rather than 12-month. The August
bill flagged 8.645 of 10 on 08/30 — under the ceiling only because the alarms were created
across 08/18–08/23 and the meter prorates by alarm-hour. A full month at 29 alarms is 19
billable alarm-months, **$1.90/month**, growing $0.30 with every source added to a map whose
whole design claim (SPEC §3) is that adding a source is one map entry.

Two of the 27 were also structurally broken. `poller_silent` hardcoded `period = 3600` for
every source, but `market` runs `cron(11 2 * * ? *)` and `macro` `cron(26 2 * * ? *)` — once
a day. Both alarms sat in ALARM roughly 23 hours out of 24, and were in ALARM when this was
written. An alarm that is always firing is not monitoring, it is a mail filter rule.

## Decision

One alarm per condition, watching the whole poller fleet, on the `AWS/Lambda` namespace with
no dimensions:

| alarm | metric | period | condition |
|---|---|---|---|
| `signal-pollers-errors` | `Errors` | 900 | `>= 1` |
| `signal-pollers-not-running` | `Invocations` | 3600 | `< 1` |
| `signal-pollers-throttled` | `Throttles` | 3600 | `>= 1` |

Plus the two local-half alarms, unchanged. **Five alarms, $0/month, and flat as sources are
added.**

`AWS/Lambda` publishes each of these at the account level with no dimensions, verified
carrying data before the change: a steady 36 `Invocations`/hour, which is `hackernews` every
5 minutes plus the six 15-minute sources, and explicit `0.0` datapoints for `Errors` and
`Throttles` rather than gaps. Account-wide is the same set as the poller fleet because
`lambda.tf`'s `poller` for_each is the only `aws_lambda_function` in `main/` — nine functions,
nine pollers.

## Consequences

**Lost: attribution.** A fired alarm says "a poller", not which one. The log groups and
`ops.source_health` (`ops/monitor.py::assess`) both say which, and the latter is the layer
SPEC §11 actually specifies for per-source health — freshness SLA, dead-feed detection,
volume anomaly. Nothing in SPEC §11 asked for per-source CloudWatch alarms; `monitoring.tf`'s
own header already called these "the backstop, not the monitoring."

**Lost: single-source silence detection.** `< 1 invocation/hour` fleet-wide means "nothing
ran at all". One dead source among nine healthy ones no longer trips CloudWatch. The floor is
36/hour and a threshold near it would catch more — and would need re-tuning on every change
to `var.sources`, which trades SPEC §3's claim for sensitivity that `ops.source_health`
already provides. That trade was refused.

**The gap this leaves is conditional on the local half.** `ops.source_health` runs in Airflow
on the laptop, so a single dead source while the laptop is also down is seen by neither. The
laptop being down is itself alarmed (`signal-local-not-running`, Phase 5), so the gap is
visible rather than silent — but it is a gap, and it is the reason the two local alarms were
kept in full while the poller alarms were collapsed.

**Re-entry criterion.** If a non-poller Lambda is ever added to `main/`, all three alarms
silently widen to cover it. That is the point to scope them with a CloudWatch Metrics Insights
query on `signal-poll-%` — which is billed per alarm rather than per matched metric, so it
holds the same five-alarm budget.
