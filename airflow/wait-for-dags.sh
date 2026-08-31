#!/usr/bin/env bash
# Refuse to run Airflow against an empty DAGs mount.
#
# Docker Desktop restarts this stack at boot, and on WSL2 it can beat the host filesystem
# to it: the bind mount then resolves to an empty directory and stays that way for the
# life of the container. Airflow comes up with zero DAGs and reports `Up` throughout,
# which is how 2026-08-30 lost eight hours of pipeline and 2026-08-31 lost the brief.
#
# This lives in the image rather than in the repo tree on purpose — a guard mounted from
# the filesystem it is checking cannot run when that filesystem is the thing missing.
#
# Exiting non-zero is the point. Every service using this runs under
# `restart: unless-stopped`, so a container that refuses to start is retried until the
# mount appears — which makes the boot race self-healing instead of merely visible. An
# earlier version of this guard lived in `airflow-init`; that only covered
# `docker compose up`, and the failure path is the daemon restarting containers directly,
# where `depends_on` is never evaluated.
set -euo pipefail

DAGS_DIR="${AIRFLOW__CORE__DAGS_FOLDER:-/opt/airflow/dags}"
ATTEMPTS="${WAIT_FOR_DAGS_ATTEMPTS:-60}"
INTERVAL="${WAIT_FOR_DAGS_INTERVAL:-5}"

for attempt in $(seq 1 "$ATTEMPTS"); do
  if compgen -G "$DAGS_DIR"/*.py > /dev/null; then
    echo "wait-for-dags: $DAGS_DIR is populated, starting $*"
    exec "$@"
  fi
  echo "wait-for-dags: $DAGS_DIR is empty (attempt $attempt/$ATTEMPTS) — waiting for the host filesystem"
  sleep "$INTERVAL"
done

echo "wait-for-dags: FATAL — $DAGS_DIR still empty after $((ATTEMPTS * INTERVAL))s." >&2
echo "wait-for-dags: refusing to start a DAG-less Airflow; the container will be restarted." >&2
exit 1
