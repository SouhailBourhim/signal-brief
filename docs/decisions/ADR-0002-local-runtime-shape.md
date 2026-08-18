# ADR-0002 — Spark in-process, Ollama native, Airflow in Docker

**Status:** Accepted · **Date:** 2026-08-18

## Context

Development runs on one machine (Ryzen 9 8940HX, 32 GB RAM, RTX 5070 8 GB VRAM). SPEC §10
keeps Spark, Airflow, and inference local because EMR, MWAA, and MSK buy nothing at this
volume. That leaves the question of how local.

## Decision

| Component | Placement |
|---|---|
| Postgres (Airflow metadata) | Docker |
| Airflow 3 (apiserver, scheduler, dag-processor) | Docker |
| Spark | in-process `local[*]`, no Compose service |
| Ollama | native on the host |

## Rationale

- **Spark**: a standalone master and worker in Compose would run the same code against the
  same `s3a` paths as `local[*]`, with two more containers to keep alive. That is the
  argument SPEC §10 already makes against EMR, applied one level down. A local cluster
  that exists to look like a cluster is the thing this project is trying not to be.
- **Ollama native**: the GPU is the reason inference is free. Reaching it from inside a
  container means the NVIDIA container toolkit and a GPU-passthrough story that adds
  failure modes without adding capability. Compose reaches the host at
  `host.docker.internal:11434`.
- **Airflow in Docker**: it is the one component whose local install genuinely is worse —
  four processes, a database, and version pinning that fights the system Python.

## Consequences

- `docker compose up` does not start everything; Ollama must be running on the host. The
  Makefile says so and `make up` prints it.
- Spark memory is bounded by the Airflow worker process, so Phase 3 must size the
  clustering job explicitly rather than assuming a cluster will absorb it.
- On Windows the whole stack requires WSL2: Airflow has no Windows support and Spark
  without WSL needs `winutils.exe`.
