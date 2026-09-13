# ADR-0020 — The brief's send window opens at 13:00, not 16:00

**Status:** Accepted · **Date:** 2026-09-13 · **Amends the clock time in ADR-0010's 2026-08-24 amendment (and the same amendment carried in ADR-0008, ADR-0011)**

## Context

Three records in a row have moved or defended this one clock time, and each was right about
the problem in front of it:

- **ADR-0010, amended 2026-08-24** moved the send from 07:00 to 16:00. The host slept through
  the early-morning slot, so the brief arrived mid-afternoon anyway; 16:00 was chosen as an
  hour the reader is demonstrably at the keyboard.
- **ADR-0014 (2026-08-29)** ordered the upstream chain by assets so the five stages could not
  race the commit they all read, and left `brief` on its cron deliberately.
- **2026-09-01** (`airflow/dags/brief_dag.py`) made `due` compare the run's own `logical_date`
  against today and skip when they differ, after a slot that thawed at 01:15 tried to build
  *today's* edition out of the previous evening's data.

Every one of those is still correct. What none of them checked is the premise underneath all
three: **that 16:00 is when this laptop is actually on.**

### What twelve days measured

Between 2026-09-01 and 2026-09-13 the brief was sent **once**, on the first of those days.
`brief_last_sent_date` stayed at `2026-09-01` for eleven days, and `out/` holds no file after
it. Nothing was broken — the `due` short-circuit was working exactly as designed, and said so:

```
slot belongs to 2026-09-11 but today is 2026-09-12 — window closed, skipping
slot belongs to 2026-09-12 but today is 2026-09-13 — window closed, skipping
```

That second line is Friday's 16:00 send slot. It did not execute until 00:52 on Saturday,
because the machine was not on before then.

So the host's real uptime was reconstructed from every timestamp Airflow leaves behind — the
`log`, `dag_run`, `task_instance` and `job` tables, plus the scheduler container's own
timestamped output (median gap between lines: 4 seconds). 27 calendar days, treating a gap over
70 minutes as the host being away, since `ingest_monitor` beats hourly at :05 and its beats
alone are evidence of an awake machine.

**The machine had uptime on 17 of the 27 days.** Ten days it was off entirely. Uptime by local
hour, over all 27:

```
13  318m ###########################
14  343m #############################
15  256m #####################
16  305m ##########################    <- window opened here
17  454m ######################################
18  375m ###############################
19  253m #####################
20  301m #########################
21  324m ###########################
22  265m ######################
23  129m ##########
```

And the number that decides it — how many days each candidate window would have caught the
host awake at all:

| Window (local) | Days caught, of 27 |
|---|---|
| 16:00–23:00 (as built) | 13 (48%) |
| 15:00–23:00 | 14 (52%) |
| 16:00–03:00 (past midnight) | 15 (56%) |
| **13:00–23:00** | **16 (59%)** |
| 13:00–03:00 | 18 (67%) |

## Decision

**The window opens at 13:00:** `schedule="0 13-23 * * *"`. Nothing else about the DAG changes —
the short-circuit, the marker, the two-task split and `catchup=False` all stand.

16:00 was never load-bearing as a *deadline*; it was a guess about the reader's day, and the
guess was three hours late. The window's own peak hour is 17:00, so its placement was sound —
it simply started after a third of the available uptime had already passed. 64% of awake
minutes fell inside 16:00–23:00; 87% fall inside 13:00–23:00.

### Why not carry the window past midnight, which looks like the better fix

It catches the two days that hurt most recently (2026-09-11 and 2026-09-12, the wake-ups at
01:14 and 01:47) and it is the wrong change, for a reason stronger than the aesthetic one
`brief_dag.py` already gives:

**`build()` renders *now*. It takes no logical date.** A run that wakes at 01:14 and sends "the
Friday brief" would deliver Saturday-01:14 content under a Friday label. That is not a late
brief, it is a mislabelled one — precisely the artifact the 2026-09-01 `due` fix was written to
prevent. Reversing it deliberately, to gain two days in a 27-day sample, trades a correctness
property for a delivery statistic.

Moving the *start* earlier costs nothing structurally: every brief stays dated and sent on its
own day, so `due` keeps working unchanged and the invariant holds.

## Consequences

- **The upstream deadline tightens from 16:00 to 13:00, and nothing has to move.** The chain
  runs `maintenance` 02:00, `market` 03:30, `resolve` 04:30, `cluster` 05:00, then `enrich`;
  ADR-0014 ordered it by assets rather than clock arithmetic, so it clears hours before either
  time. The slack drops from about ten hours to about seven.
- **No code outside the schedule depends on the hour.** `ops/streak.py`'s
  `LIVE_TOLERANCE_DAYS = 1` is day-granular, and `enrich_dag`'s capacity bound is an absolute
  per-head budget (`CAPACITY_SECONDS_PER_HEAD`), not a deadline computed backward from the
  send. Both carried prose naming 16:00; the prose was updated, the arithmetic was not.
- **SPEC §12's acceptance text changes**, from "emailed at 16:00" to a window opening at 13:00.
  This is the acceptance criterion for 4A, so it is a real amendment rather than a doc tidy.
- **This does not fix the laptop.** Ten of twenty-seven days had no uptime at all, and no
  schedule reaches those. The window is worth roughly 60% of days; the remaining gap is
  ADR-0002's bet that the interpretive half can live on a machine that closes, and this is the
  first measurement of what that bet costs. It is not yet an argument to reverse ADR-0002 —
  but it is the number to weigh when that comes up.
- The 16:00 references scattered through the runbooks and archived design notes are left as
  written. They record what was true when written, and ADR-0010's amendment already
  established that pattern; this record is the pointer.
