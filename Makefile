# Signal — see SPEC.md. Every target here is meant to work from a fresh clone.
.DEFAULT_GOAL := help
.PHONY: help setup up down skeleton test lint fmt eval brief clean tf-validate

UV ?= uv

help: ## show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup: ## install the project and dev dependencies
	$(UV) sync --all-extras
	$(UV) run pre-commit install

up: ## start Airflow + Postgres (Ollama runs natively on the host — ADR-0002)
	docker compose up -d
	@echo "Airflow: http://localhost:8080"

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

eval: ## score the labeled eval sets and enforce the accuracy floors (SPEC 11)
	$(UV) run python evals/score.py --gate

brief: ## open the most recent rendered brief
	@ls -t out/brief-*.html | head -1 | xargs -I{} sh -c 'echo {}; open {} 2>/dev/null || true'

tf-validate: ## terraform fmt + validate
	terraform -chdir=infra/terraform/bootstrap fmt -check
	terraform -chdir=infra/terraform/main fmt -check
	terraform -chdir=infra/terraform/main init -backend=false && \
		terraform -chdir=infra/terraform/main validate

clean: ## remove generated data and briefs (never touches bronze in S3)
	rm -rf data out .cache .pytest_cache .ruff_cache .mypy_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
