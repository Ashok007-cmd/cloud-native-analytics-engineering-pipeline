SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.PHONY: help setup bootstrap install ingest build build-models test test-py test-all validate validate-all freshness docs lint lint-py lint-sql backup restore airflow-up airflow-down clean check-dbt check-python check-env security

help:
	@echo "Available targets:"
	@echo "  setup         One-time developer setup: pip install, pre-commit, dbt deps"
	@echo "  bootstrap     Generate Airflow .env file with a fresh Fernet key"
	@echo "  install       Install dbt dependencies (dbt deps)"
	@echo "  ingest        Load CSV into DuckDB raw layer"
	@echo "  build         Full pipeline: install + ingest + dbt build"
	@echo "  build-models  Run dbt build only (skips dbt deps and ingest)"
	@echo "  test          Run dbt tests only"
	@echo "  test-py       Run Python unit tests (pytest tests/)"
	@echo "  test-all      Run dbt tests + Python unit tests"
	@echo "  validate      Lint + test-all (full pre-merge check)"
	@echo "  validate-all  Lint + test-all + security audit (pre-merge + CVE check)"
	@echo "  freshness     Check dbt source freshness"
	@echo "  docs          Generate and serve dbt docs (opens browser)"
	@echo "  lint          Run all linters (SQL + Python)"
	@echo "  lint-sql      Run SQLFluff on dbt models"
	@echo "  lint-py       Run ruff on Python scripts and DAG"
	@echo "  backup        Backup DuckDB database with sha256 checksum (non-destructive)"
	@echo "  restore       Restore DuckDB from a backup: make restore FILE=backups/<name>.tar.gz"
	@echo "  airflow-up    Start Airflow via Docker Compose"
	@echo "  airflow-down  Stop Airflow"
	@echo "  clean         Remove dbt artifacts and DuckDB databases"

# ── Environment validation ──────────────────────────────────────────────────

.PHONY: check-dbt check-python

check-dbt:
	@command -v dbt >/dev/null 2>&1 || { echo "ERROR: dbt not found — install dbt-duckdb"; exit 1; }

check-python:
	@command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }

check-env: check-dbt check-python

# ── Targets ─────────────────────────────────────────────────────────────────

setup:
	pip install --require-hashes -r requirements-dev.txt
	pre-commit install
	cd dbt_project && dbt deps

bootstrap:
	bash scripts/bootstrap_env.sh

install: check-dbt
	cd dbt_project && [ -d dbt_packages ] || dbt deps

ingest: check-python
	python scripts/ingest_raw.py

build: check-env install ingest
	cd dbt_project && dbt build --profiles-dir .

build-models: check-dbt
	cd dbt_project && dbt build --profiles-dir .

test: check-dbt
	cd dbt_project && dbt test --profiles-dir .

test-py: check-python
	pytest tests/ -v --tb=short

test-all: test test-py

validate: lint test-all

validate-all: lint test-all security

freshness: check-dbt
	cd dbt_project && dbt source freshness --profiles-dir .

docs: check-dbt
	cd dbt_project && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

lint: lint-sql lint-py

lint-sql:
	sqlfluff lint dbt_project/models/ dbt_project/snapshots/ --dialect duckdb

lint-py:
	ruff check scripts/ airflow/dags/

security:
	@echo "--- Running pip-audit ---"
	pip-audit -r requirements-dev.txt || echo "::warning::Vulnerabilities found — review pip-audit output above"
	@echo "--- Running detect-secrets ---"
	# Prefer `git ls-files` (respects .gitignore, and its paths match .secrets.baseline's
	# format exactly). Falls back to `find` (note: -printf '%P' not -print, so paths don't
	# get a leading './' that would fail to match baseline entries keyed without it) when
	# not in a git repo.
	detect-secrets-hook --baseline .secrets.baseline $$(git ls-files 2>/dev/null || find . \( -path ./dbt_project/dbt_packages -o -path ./dbt_project/target -o -path ./dbt_project/logs -o -path ./airflow/logs -o -path ./.git -o -name .venv -o -name venv -o -name __pycache__ -o -name node_modules -o -name '*.duckdb' -o -name '*.csv' \) -prune -o -type f -printf '%P\n') && echo "No new secrets found"

backup:
	bash scripts/backup.sh

restore:
	@[ -n "$(FILE)" ] || { echo "Usage: make restore FILE=backups/dev.duckdb.<timestamp>.tar.gz"; exit 1; }
	bash scripts/restore.sh "$(FILE)"

airflow-up:
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down

clean:
	cd dbt_project && dbt clean --profiles-dir .
