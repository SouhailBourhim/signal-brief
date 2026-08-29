"""Phase 4A DAG: build the brief and mail it at 16:00. SPEC §12; ADR-0002, ADR-0010.

**Cron, not asset-triggered**, and this is the one scheduling decision in the phase worth
arguing. `assets.py` anticipates the brief consuming `CLUSTERS_COMMITTED`, and the dependency
is real — but SPEC §12's acceptance says "emailed at **16:00**", which is a clock time, not
"whenever clustering last finished". Asset-triggering would also risk a second send if either
upstream table were rebuilt later the same day by a manual trigger or a backfill, and a
duplicate brief in the inbox is worse than a late one.

So the assets are declared as inlets for graph visibility, exactly as `cluster_dag` and
`resolve_dag` already do with `SILVER_COMMITTED`, and the schedule is a cron in the reader's
timezone.

**16:00 rather than the former early-morning schedule, and the reason is the host, not
the pipeline.**
ADR-0002 puts everything interpretive on a laptop, and a laptop sleeps. On 2026-08-24 the
scheduled slot passed with the machine suspended: the scheduler logged nothing between 21:00
and 12:58 UTC, then resumed mid-stride and fired the whole chain at once, so the brief
landed at 13:59 — the second day running. A `restart:` policy does not help, because the
containers never died; they were frozen with the host, which reports them as `Up` the
whole time.

A clock time only works if the machine is awake to see it, so the send moved to an hour the
reader is demonstrably at the keyboard. The upstreams are deliberately left at 02:00 to
06:15: they catch up whenever the host wakes, and that now has until 16:00 to happen rather
than the 45 minutes it had behind `enrich`. The wider gap is the point, not a side effect.

Two tasks rather than one, because they fail differently and only one of them is worth
retrying: a build failure is a data or query problem, while a send failure is a credential or
a connection (see `infra/terraform/main/mail.tf`). Splitting them also means the rendered file
survives a send failure — `make brief-open` still works, and the morning is not lost.

**What a green `mail` task does and does not prove (2026-08-28).** Until ADR-0013 it proved
only that SES had accepted the message. It did that five times while every brief sat in the
reader's Spam folder, because a `From:` of `gmail.com` sent via `amazonses.com` aligns for
neither SPF nor DKIM, and Gmail quarantines such mail rather than bouncing it. The send now
goes through Gmail's own submission service, where a `250` is the account handing a message
to itself — so green here is worth more than it was, but the acceptance test is still a brief
that was *read*, not a task that was green.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from alerting import DEFAULT_ARGS, on_dag_success
from assets import CLUSTERS_COMMITTED, ENRICHMENT_READY, MENTIONS_RESOLVED


@dag(
    dag_id="brief",
    schedule="0 16 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_success_callback=on_dag_success,
    tags=["phase4a", "brief"],
)
def brief_dag():
    @task(inlets=[CLUSTERS_COMMITTED, ENRICHMENT_READY, MENTIONS_RESOLVED])
    def build() -> str:
        """Rank and render, writing `gold.brief_items` on the way through."""
        from signal_core.brief.build import run

        return str(run())

    @task(retries=2, retry_delay=pendulum.duration(minutes=5))
    def mail(path: str) -> str:
        """Send the file `build` just wrote — not a fresh render.

        Re-rendering here would let the emailed brief differ from the one on disk, and the
        acceptance test rests on the reader and the record being the same artifact.

        Retried, unlike `build`: a send failure is usually a transient SMTP connection or an
        app password that has not been set yet (`mailer.py` names the parameter when it is
        still the Terraform placeholder), and the first of those is worth a second attempt a
        few minutes later.
        """
        from pathlib import Path

        from signal_core.brief.mailer import send_brief_file

        message_id = send_brief_file(Path(path))
        print(f"sent {path} -> {message_id}")
        return message_id

    mail(build())


brief_dag()
