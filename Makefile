# Signal — see SPEC.md. Every target here is meant to work from a fresh clone.
.DEFAULT_GOAL := help
.PHONY: help setup up down skeleton test lint fmt eval brief brief-open clean tf-validate lambda-package airflow-password athena-query

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

test: ## run the test suite
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

clean: ## remove generated data and briefs (never touches bronze in S3)
	@# .cache is bind-mounted into every Airflow container (docker-compose.yml). Deleting
	@# it while `make up` is running breaks that mount until the containers are recreated
	@# (docs/runbooks/phase-2.md 2.E) — `docker compose up -d --force-recreate
	@# airflow-scheduler airflow-apiserver airflow-dag-processor` if it happens.
	rm -rf data out build .cache .pytest_cache .ruff_cache .mypy_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
