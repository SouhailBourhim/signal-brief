"""Phase 4A DAG: build the brief and mail it at 07:00. SPEC §12; ADR-0002, ADR-0010.

**Cron, not asset-triggered**, and this is the one scheduling decision in the phase worth
arguing. `assets.py` anticipates the brief consuming `CLUSTERS_COMMITTED`, and the dependency
is real — but SPEC §12's acceptance says "emailed at **07:00**", which is a clock time, not
"whenever clustering last finished". Asset-triggering would also risk a second send if either
upstream table were rebuilt later the same morning by a manual trigger or a backfill, and a
duplicate brief in the inbox is worse than a late one.

So the assets are declared as inlets for graph visibility, exactly as `cluster_dag` and
`resolve_dag` already do with `SILVER_COMMITTED`, and the schedule is a cron in the reader's
timezone.

Two tasks rather than one, because they fail differently and only one of them is worth
retrying: a build failure is a data or query problem, while a send failure is usually the
SES identity not being verified (see `infra/terraform/main/mail.tf`). Splitting them also
means the rendered file survives a send failure — `make brief-open` still works, and the
morning is not lost.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from assets import CLUSTERS_COMMITTED, ENRICHMENT_READY, MENTIONS_RESOLVED


@dag(
    dag_id="brief",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="Africa/Casablanca"),
    catchup=False,
    max_active_runs=1,
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

        Retried, unlike `build`: a send failure is usually transient or an unverified SES
        identity, and both are worth a second attempt a few minutes later.
        """
        from pathlib import Path

        from signal_core.brief.mailer import send_brief_file

        message_id = send_brief_file(Path(path))
        print(f"sent {path} -> {message_id}")
        return message_id

    mail(build())


brief_dag()
