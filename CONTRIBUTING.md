# Contributing

## Setup

```bash
git clone <repo-url>
cd cloud-native-analytics-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pre-commit install
```

## Running the pipeline

```bash
make ingest        # load CSV → raw.online_retail (requires data/online_retail_II.csv)
make build         # dbt deps + ingest + dbt build (run + test)
make build-models  # dbt build only (skip ingest, reuse existing raw data)
make test          # dbt tests only
make docs          # generate + serve dbt docs at http://localhost:8080
```

## Code style

- **SQL**: SQLFluff (DuckDB dialect). Run `make lint-sql` before pushing.
- **Python**: ruff. Run `make lint-py` before pushing.
- pre-commit hooks enforce both automatically on `git commit`.

## Adding a dbt model

1. Create SQL in the correct layer: `models/staging/`, `models/intermediate/`, or `models/marts/`
2. Add a model block in `models/schema.yml` with description and column tests
3. Run `make build-models` to verify it compiles and passes tests
4. Run `make lint-sql` to check SQL style

## Adding a dbt test

- Generic tests: add to the model's `columns:` block in `schema.yml`
- Singular tests: add a `.sql` file to `dbt_project/tests/` that returns rows on failure

## RFM segmentation thresholds

Thresholds are parameterised in `dbt_project.yml` under `vars:`. Override per run:

```bash
dbt build --profiles-dir . --vars '{"rfm_high_value_min_orders": 5, "rfm_high_value_min_revenue": 500}'
```

## Airflow (local)

```bash
cp airflow/.env.example airflow/.env   # fill in FERNET_KEY, passwords
make airflow-up                        # starts Airflow at http://localhost:8080
make airflow-down
```

## Python tests

Unit tests for pipeline scripts live in `tests/`. Run them with:

```bash
pip install pytest
pytest tests/ -v
```

Tests use `tmp_path` fixtures so they don't touch the project database or data files.

## Pull requests

- Branch from `main`, name it `feat/...`, `fix/...`, or `chore/...`
- Keep PRs focused — one logical change per PR
- CI must be green before merge
