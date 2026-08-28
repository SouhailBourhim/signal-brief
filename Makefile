# Signal — see SPEC.md. Every target here is meant to work from a fresh clone.
.DEFAULT_GOAL := help
.PHONY: help setup up down skeleton test test-fast lint fmt eval brief brief-open clean tf-validate lambda-package airflow-password athena-query

UV ?= uv

help: ## show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup: ## install the project and dev dependencies
	$(UV) sync --all-extras
	$(UV) run pre-commit install

up: ## start Airflow + Postgres (Ollama runs natively on the host — ADR-0002)
	docker compose up -d
	@echo "Airflow: http://localhost:8080 — user 'admin', password from 'make airflow-password'"

airflow-password: ## print the Airflow UI password (SimpleAuthManager generates it)
	@# Airflow 3 generates this into AIRFLOW_HOME on first start and regenerates it
	@# whenever the container is recreated, so it is read, never written down.
	@docker compose exec -T airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated

down: ## stop local services
	docker compose down

skeleton: ## Phase 0 walking skeleton: fake source -> bronze -> silver -> brief
	$(UV) run signal skeleton

skeleton-nospark: ## same, without a JVM (transport differs, logic is identical)
	$(UV) run signal skeleton --no-spark

test: ## run the test suite with the coverage gate
	$(UV) run pytest --cov=signal_core --cov-report=term-missing

test-fast: ## the suite without coverage, for a tight local loop
	$(UV) run pytest

lint: ## ruff check + format check + mypy
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy src

fmt: ## autoformat
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

dictionary: ## rebuild warehouse/entities/dictionary.json.gz from SEC + Wikidata (network)
	@# The only networked build step in the repo, and it is deliberately manual: the output
	@# is committed so `make eval` stays offline and reproducible (SPEC 7.2). Takes ~30 min,
	@# most of it Wikidata's rate limit. --skip-wikidata rebuilds the SEC tier in seconds.
	$(UV) run python -m signal_core.entities.build

eval: ## score the labeled eval sets and enforce the accuracy floors (SPEC 11)
	$(UV) run python evals/score.py --gate

brief: ## build today's brief from real silver.articles, then open it
	$(UV) run signal brief
	@$(MAKE) --no-print-directory brief-open

brief-open: ## open the most recent rendered brief without rebuilding
	@# wslview first: this is a WSL2 project (ADR-0002) and `open` is macOS-only, so the
	@# original one-liner silently printed a path and opened nothing on the machine the
	@# brief is actually read on every morning. Phase 3's acceptance is a reading habit.
	@ls -t out/brief-*.html | head -1 | xargs -I{} sh -c \
		'echo {}; wslview {} 2>/dev/null || xdg-open {} 2>/dev/null || open {} 2>/dev/null || true'

athena-query: ## run Q="SELECT ..." [DB=bronze|silver|ops, default silver] against the lake
	$(UV) run signal athena-query --sql "$(Q)" $(if $(DB),--database $(DB))

lambda-package: ## build build/lambda/, the poller deployment artifact Terraform zips
	rm -rf build/lambda
	# Linux wheels for the Lambda runtime, not this machine's — pydantic-core is
	# compiled, so a macOS or Windows wheel here fails at import time in AWS with a
	# stack trace that names nothing useful. ADR-0006.
	$(UV) pip install --target build/lambda --only-binary :all: \
		--python-platform x86_64-manylinux2014 --python-version 3.12 \
		httpx pydantic pydantic-settings
	cp -r src/signal_core build/lambda/
	# Flat, so the AWS handler string is `poll_source.handler` (see infra lambda.tf).
	cp handlers/poll_source.py build/lambda/
	find build/lambda -name '__pycache__' -type d -prune -exec rm -rf {} +
	@du -sh build/lambda

tf-validate: ## terraform fmt + validate
	terraform -chdir=infra/terraform/bootstrap fmt -check
	terraform -chdir=infra/terraform/main fmt -check
	terraform -chdir=infra/terraform/main init -backend=false && \
		terraform -chdir=infra/terraform/main validate

# Every host path `docker-compose.yml` bind-mounts into the Airflow containers, except
# `./src` and `./airflow/dags` which `clean` has no business touching.
#
# Deleting one of these while the containers are up breaks the mount **at the inode level**:
# the container's view survives as a directory with link count 0, and every `mkdir` inside it
# fails with ENOENT. Recreating the directory on the host does NOT fix a running container —
# only recreating the container re-establishes the mount.
#
# Keep this in step with the `volumes:` block. Missing one is not a small mistake: omitting
# `.cache` cost ten hours of silently failed `ingest_monitor` runs on 2026-08-22, and the
# first version of this guard covered `.cache` alone and promptly broke `out` and `data` the
# same way — a brief the 16:00 DAG would have failed to write. See docs/runbooks/phase-4b.md.
MOUNTED_PATHS := data out .cache

# Generated, and mounted nowhere. Safe to delete whatever is running.
UNMOUNTED_PATHS := build .pytest_cache .ruff_cache .mypy_cache

clean: ## remove generated data and briefs (never touches bronze in S3)
	@if docker compose ps --services --filter status=running 2>/dev/null | grep -q airflow; then \
		echo "Airflow is up — keeping $(MOUNTED_PATHS), which are bind-mounted into the"; \
		echo "containers. Deleting them now breaks those mounts until the containers are"; \
		echo "recreated, which stops ingestion and the 16:00 brief silently."; \
		echo "Run 'make down' first, or 'make clean-mounted' to delete them and recreate."; \
		rm -rf $(UNMOUNTED_PATHS); \
	else \
		rm -rf $(MOUNTED_PATHS) $(UNMOUNTED_PATHS); \
	fi
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

clean-mounted: ## delete the bind-mounted dirs even with Airflow up, recreating the containers
	rm -rf $(MOUNTED_PATHS)
	@# Order matters: the host directories have to exist before the containers are recreated,
	@# or Docker creates them root-owned and the containers cannot write to them.
	mkdir -p $(MOUNTED_PATHS) .cache/ivy2
	docker compose up -d --force-recreate airflow-scheduler airflow-apiserver airflow-dag-processor
