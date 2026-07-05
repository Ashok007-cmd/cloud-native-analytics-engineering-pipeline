<!-- generated-by: gsd-doc-writer -->
# Development Guide: Cloud-Native Analytics Engineering Pipeline

## Prerequisites

| Tool | Version | Required For |
|------|---------|-------------|
| **Python** | >= 3.11, < 3.13 | All development (ingestion, dbt, testing) |
| **pip** | Latest | Dependency management |
| **Make** | >= 4.0 | Command shortcuts |
| **Docker** | >= 24.0 + Docker Compose v2 | Airflow orchestration |
| **Git** | >= 2.30 | Version control |

### Optional

| Tool | Version | Required For |
|------|---------|-------------|
| **Snowflake account** | — | Production deployment |
| **GitHub Pages** | — | dbt docs hosting |
| **Slack webhook** | — | Airflow failure alerts |

### Version Requirements (from source)

- **Python**: `pyproject.toml` requires `requires-python = ">=3.11,<3.13"`
- **DuckDB**: `dbt-duckdb` 1.9.x (pinned in `requirements-dev.txt`)
- **Snowflake Connector**: `snowflake-connector-python` >= 3.12.3
- **Airflow**: 2.10.4 (pinned in `docker-compose.yaml`)
- **PostgreSQL**: 16 (pinned in `docker-compose.yaml`)

## Local Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Create and Activate Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

### 3. Install Dependencies

**Install from lockfile (recommended for reproducible builds):**

```bash
pip install -r requirements-dev.txt
```

**Install from loose specifier (for upgrades):**

```bash
pip install -r requirements-dev.in
```

The `requirements-dev.txt` lockfile is generated from `requirements-dev.in` and includes:
- **dbt-core** 1.8.9, **dbt-duckdb** 1.9.1, **dbt-snowflake** 1.8.4
- **pandas**, **pyarrow** (data handling)
- **pytest**, **pytest-cov**, **pytest-xdist** (testing)
- **ruff**, **mypy** (linting/typing)
- **sqlfluff** + **sqlfluff-templater-dbt** (SQL linting)

### 4. Install Pre-commit Hooks

```bash
pre-commit install
```

This installs the hooks defined in `.pre-commit-config.yaml`:
| Hook | Purpose |
|------|---------|
| `ruff check` | Python linting (auto-fix) |
| `ruff format` | Python formatting |
| `detect-secrets` | Scans for committed secrets |
| `mixed-line-ending` | Normalizes line endings |
| `trailing-whitespace` | Removes trailing whitespace |
| `end-of-file-fixer` | Ensures newline at EOF |

### 5. Verify Configuration

```bash
make lint
```

This runs:
- `ruff check .` (Python lint)
- `ruff format --check .` (Python formatting)
- `sqlfluff lint dbt_project/` (SQL lint — DuckDB dialect)
- `pip-audit` (CVE scanning of installed packages)

### 6. Run the Full Pipeline Locally

```bash
make build
```

This single command:
1. Runs `dbt deps` — installs dbt packages (`dbt_utils`, `codegen`, `dbt_expectations`)
2. Runs `scripts/ingest_raw.py` — validates and loads CSV data into DuckDB `raw.online_retail`
3. Runs `dbt build` — executes all models, tests, snapshots, and source freshness checks

The DuckDB database is created at the path specified by `DUCKDB_PATH` (default: `dev.duckdb` in the project root).

## Adding a New Data Source

### Step-by-Step

Assume you want to add a new source table called `new_raw_table`.

#### 1. Add to `sources.yml`

In `dbt_project/models/sources.yml`:

```yaml
sources:
  - name: raw
    database: raw
    schema: raw
    tables:
      - name: online_retail
        # existing...
      - name: new_raw_table
        description: "New data source for ..."
        loaded_at_field: processed_at
        freshness:
          warn_after: { count: 12, period: hour }
          error_after: { count: 24, period: hour }
        columns:
          - name: id
            description: "Primary key"
            data_type: integer
          - name: value
            description: "Numeric value"
            data_type: numeric
```

#### 2. Create Staging Model

`dbt_project/models/staging/stg_new_raw_table.sql`:

```sql
with source as (
    select * from {{ source('raw', 'new_raw_table') }}
),

cleaned as (
    select
        id,
        value,
        current_timestamp as _loaded_at
    from source
    where id is not null
)

select * from cleaned
```

#### 3. Add Schema Tests

In `dbt_project/models/schema.yml`, add to the `staging` section:

```yaml
- name: stg_new_raw_table
  description: "Cleaned version of raw.new_raw_table"
  columns:
    - name: id
      data_tests:
        - unique
        - not_null
    - name: value
      data_tests:
        - not_null
```

#### 4. Update Ingestion Script

Edit `scripts/ingest_raw.py` to add the new CSV loading logic following the `online_retail` pattern:

- Add CSV validation for the new file.
- Extend `SQL_CREATE_RAW` template or create a new template.
- Insert rows using `executemany`.

#### 5. Wire into Orchestration

No changes needed to the Airflow DAG — `dbt build` auto-discovers new models by scanning the `dbt_project/` directory. The DAG in `dbt_cosmos_dag.py` uses `DbtDag` which renders tasks from the dbt manifest.

## Modifying Existing Models

### Change a Staging Model (`stg_*.sql`)

1. Edit the SQL in `dbt_project/models/staging/`.
2. Change the `schema.yml` entry if modifying column names or adding tests.
3. If changing the final output columns, update downstream intermediate/mart models that `ref()` this model.
4. Run:
   ```bash
   dbt run --select stg_online_retail+
   ```
   The `+` suffix includes all downstream dependencies.

### Change an Intermediate Model

1. Edit in `dbt_project/models/intermediate/`.
2. Update schema tests in `schema.yml`.
3. If you change the config (e.g., materialization), update accordingly.
4. Run:
   ```bash
   dbt run --select int_customer_metrics+ --full-refresh
   ```

### Change a Mart Model

1. Edit in `dbt_project/models/marts/`.
2. The `fct_orders` model uses incremental materialization; changes to SQL require a full-refresh:
   ```bash
   dbt run --select fct_orders --full-refresh
   ```
3. Modify `dim_customers`, `dim_products`, or `dim_dates`:
   ```bash
   dbt run --select dim_customers
   ```
4. **Note**: `dim_customers` and `dim_products` filter to `dbt_valid_to IS NULL` on top of SCD2 snapshots. If the snapshot structure changes, you must rebuild the snapshot first.

### Change a Snapshot Model

1. Edit in `dbt_project/snapshots/`.
2. Snapshots use `strategy: check` — changes to the `check_cols` list change which columns trigger new versions.
3. **Adding a column**: Adding to `check_cols` will create new version rows for customers/products where that column changed.
4. **Removing a column**: Removing from `check_cols` will not retroactively fix existing rows (old versions remain). Use `--full-refresh` on snapshots only in development:
   ```bash
   dbt snapshot --full-refresh
   ```
   **Warning**: Full-refreshing snapshots in production destroys historical tracking.

## Makefile Reference

All common operations are wrapped in the **Makefile** at project root.

| Command | Description | Dependencies |
|---------|-------------|-------------|
| `make install` | Install Python deps from `requirements-dev.txt` | `.venv` |
| `make setup` | Full setup: venv, deps, pre-commit, ingest, dbt-deps | `pyproject.toml` |
| `make ingest` | Load CSV into DuckDB | `data/*.csv` |
| `make dbt-deps` | Install dbt packages | `dbt_project/packages.yml` |
| `make build` | Full pipeline: `dbt-deps → ingest → dbt-build` | All above |
| `make run` | `dbt run` only | `make dbt-deps` |
| `make test` | `dbt test` + `pytest tests/` | `make run` |
| `make lint` | Python lint + format check + SQL lint + pip-audit | `.venv` |
| `make format` | Auto-format Python + SQL | `.venv` |
| `make clean` | Remove DuckDB database, Python cache, generated docs | — |
| `make backup` | Archive `dbt_project/` to `target/backup/` | `scripts/backup.sh` |
| `make fresh` | `dbt source freshness` | `make dbt-deps` |
| `make docs` | Generate dbt docs | `make build` |
| `make serve-docs` | Serve dbt docs on port 8081 | `make docs` |
| `make docker-up` | Start Airflow stack | Docker |
| `make docker-down` | Stop Airflow stack | Docker |
| `make docker-logs` | Tail Airflow logs | Docker |

### Makefile Implementation Notes

- The Makefile auto-detects Python virtual environment (checks various paths).
- The `build` target ensures `dbt-deps` runs before build to guarantee packages are installed.
- `backup` uses `scripts/backup.sh` which creates `tar.gz` archives with SHA-256 checksums.

## Testing Guide

### dbt Tests

**Types of tests**:

| Test Type | Count | What It Tests |
|-----------|-------|---------------|
| **Schema (generic)** | 70+ | `not_null`, `unique`, `accepted_values`, `relationships`, `dbt_utils.*`, `dbt_expectations.*` |
| **Singular** | 2 | `assert_no_high_value_customers_with_negative_revenue.sql` — business logic integrity; `assert_fct_orders_row_count_matches_staging.sql` — row count parity |
| **Source freshness** | 2 thresholds | Warn @ 24h, Error @ 48h for `raw.online_retail` |

**Run all dbt tests:**

```bash
make test        # dbt test + pytest
dbt test         # dbt tests only
```

**Run specific test:**

```bash
dbt test --select test_name:assert_no_high_value_customers_with_negative_revenue
dbt test --select stg_online_retail     # all tests on staging model
```

**Run tests for a specific model group:**

```bash
dbt test --select tag:mart              # all mart model tests
dbt test --select tag:staging           # all staging model tests
```

### Python Tests

File: `tests/test_scripts.py`

```bash
pytest tests/            # run all Python tests
pytest -v tests/         # verbose
pytest --cov=. tests/    # with coverage (configured in pyproject.toml)
```

Coverage configuration from `pyproject.toml`:
```toml
[tool.coverage.run]
source = ["scripts"]
relative_files = true
[tool.coverage.report]
fail_under = 80
```

### Test Data

The CI pipeline uses a synthetic 3,000-row CSV generated by `scripts/ci_setup.py`:

```python
# Generate same sample locally
python scripts/ci_setup.py
```

Output: `data/online_retail_II_sample_ci.csv`

The sample is seeded (random seed 42) and includes:
- 15 customers, 105 products.
- ~1–5 rows per product, ~200 orders per customer.
- ~2.2% of rows flagged as returns.
- One low-revenue customer to test the high-value filter.

### Testing Macros

Located in `tests/` directory (singular test SQL files). Custom macros are tested indirectly through the models that use them:

| Macro | Tested Via | Location |
|-------|-----------|----------|
| `calculate_gross_revenue` | `dim_customers.total_revenue` value (should exclude returns) | `models/intermediate/int_customer_metrics.sql` |
| `dayofweek_expression` | `int_orders.order_day_of_week` output | `models/intermediate/int_orders.sql` |
| `test_positive_value` | `dim_customers.total_revenue` must be >= 0 | `models/staging/stg_online_retail.sql` |
| `test_not_in_future` | `dim_dates.date_day` must be <= `invoice_date` max | `models/marts/dim_dates.sql` |

## CI/CD Pipeline

### CI Pipeline (`ci.yml`)

**Trigger**: Pull request to `main`, push to non-`main` branches.

**Steps**:
1. Checkout repository.
2. Set up Python 3.11, cache pip.
3. Install dependencies (pins from `requirements-dev.txt`).
4. Security scan: `pip-audit`, `detect-secrets`.
5. Lint: `ruff check`, `ruff format --check`, `sqlfluff lint`.
6. Generate synthetic sample: `python scripts/ci_setup.py`.
7. Run Python tests: `pytest tests/ --tb=short`.
8. dbt build on DuckDB (`ci.duckdb`, separate from dev database):
   - `dbt deps`
   - `python scripts/ingest_raw.py --csv data/online_retail_II_sample_ci.csv`
   - `dbt build`
9. (Optional) Snowflake CI: runs only if triggered from the canonical repository and `SNOWFLAKE_CI_ENABLED` is set. Uses `--select state:modified+ --defer --state target/` for efficient incremental CI.

The CI step caches dbt packages with key `dbt-packages-${{ hashFiles('dbt_project/packages.yml') }}`.

### CD Pipeline (`cd.yml`)

**Trigger**: Push to `main`.

**Steps**:
1. Checkout repository.
2. Set up Python 3.11.
3. Install `dbt-snowflake`.
4. Validate Snowflake connection (`dbt debug --target prod`).
5. Full production build: `dbt build --target prod`.

### Docs Pipeline (`docs.yml`)

**Trigger**: Push to `main`.

**Steps**:
1. Checkout.
2. Set up Python 3.11.
3. Install dbt-duckdb + deps.
4. Generate synthetic sample + build docs: `dbt deps && python scripts/ci_setup.py && dbt docs generate`.
5. Deploy to GitHub Pages (uses `actions/configure-pages` + `actions/upload-pages-artifact` + `actions/deploy-pages`).

## Adding a New Test

### Generic (Schema) Test

Add to `dbt_project/models/schema.yml` under the appropriate model:

```yaml
- name: dim_customers
  columns:
    - name: customer_segment
      data_tests:
        - accepted_values:
            values: ['high', 'medium', 'low', 'unknown']
```

### Singular Test

Create a new `.sql` file in `dbt_project/tests/`:

```sql
-- tests/assert_orders_have_valid_dates.sql
WITH invalid_dates AS (
    SELECT order_id
    FROM {{ ref('fct_orders') }}
    WHERE order_date IS NULL
       OR order_date < '2009-01-01'
       OR order_date > CURRENT_DATE
)
SELECT * FROM invalid_dates
-- This test passes when zero rows are returned
```

### Custom Generic Test (via Macro)

Create a macro in `dbt_project/macros/`:

```sql
{% macro test_no_future_dates(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} > CURRENT_TIMESTAMP
{% endmacro %}
```

Then use it in `schema.yml`:

```yaml
- name: fct_orders
  columns:
    - name: order_date
      data_tests:
        - no_future_dates
```

## Troubleshooting

### Common Issues

| Symptom | Likely Cause | Solution |
|---------|-------------|----------|
| `dbt: command not found` | Virtual environment not activated | `source .venv/bin/activate` |
| `No module named 'dbt'` | Dependencies not installed | `pip install -r requirements-dev.txt` |
| `Error: Database "ci.duckdb" does not exist` | CI database not created | CI creates it automatically during `dbt deps`; locally use `make build` |
| Source freshness warning | CSV hasn't been re-ingested | `python scripts/ingest_raw.py && dbt source freshness` |
| `duckdb.IOException` on ingest | File locked by another DuckDB process | Close other DuckDB connections (dbt shell, DBeaver). DuckDB is single-writer. |
| `Detected secrets in .secrets.baseline` | `detect-secrets` found a new credential | `detect-secrets scan > .secrets.baseline` (after verifying it's not a real secret) |
| SQLFluff fails on DuckDB syntax | SQLFluff dialect mismatch | Ensure `dialect = duckdb` in `setup.cfg` (already configured) |
| Airflow webserver won't start | Port 8080 in use | Change port or kill the existing process: `docker compose down` |
| `duckdb_pool` not found in Airflow | Pool not created in metastore | Run `docker compose run --rm airflow-cli airflow pools set duckdb_pool 1 "DuckDB pool"` |
| `dbt build` fails on incremental model | Incremental logic mismatch | `dbt run --select fct_orders --full-refresh` |
| `dbt snapshot` duplicates rows | Check column changed value | Verify `check_cols` in snapshot YAML. Use `SELECT * ... QUALIFY ROW_NUMBER() ...` to dedup. |

### Debugging the Airflow DAG

1. Check task logs in Airflow UI at `http://127.0.0.1:8080`.
2. Check the DAG failure log:
   ```bash
   docker compose exec airflow-webserver cat /opt/airflow/logs/dag_failures.log
   ```
3. Trigger a manual run:
   ```bash
   docker compose exec airflow-webserver airflow dags trigger online_retail_elt
   ```
4. Test dbt profile resolution:
   ```bash
   docker compose exec airflow-webserver dbt debug --project-dir /opt/airflow/dbt_project
   ```

### Debugging the Ingest Script

```bash
# Run with verbose logging
python scripts/ingest_raw.py --csv data/online_retail_II.csv --verbose

# Test with minimum row count check
INGEST_MIN_ROWS=100 python scripts/ingest_raw.py --csv data/online_retail_II.csv

# Ingest into Snowflake (requires env vars set)
python scripts/ingest_raw.py --target snowflake
```

### Debugging dbt

```bash
# Show generated SQL without executing
dbt compile
cat target/compiled/dbt_project/models/marts/fct_orders.sql

# Run with debug logging
dbt --debug build

# Show the full dependency graph
dbt ls --resource-type model

# List all tests
dbt ls --resource-type test
```

## Custom Macros Reference

| Macro | Parameters | Purpose | File |
|-------|-----------|---------|------|
| `calculate_gross_revenue(quantity, unit_price, is_return)` | 3 columns | `SUM(quantity * unit_price * CASE WHEN is_return THEN 0 ELSE 1 END)` | `macros/revenue_calculations.sql` |
| `dayofweek_expression(date_column)` | 1 column | Cross-DB weekday name. Uses `EXTRACT(ISODOW...)CASE` for DuckDB, `DAYNAME()` for Snowflake | `macros/cross_database.sql` |
| `test_positive_value(model, column_name)` | model, column_name | Ensures all values in a column are >= 0 | `macros/validation_macros.sql` |
| `test_not_in_future(model, column_name)` | model, column_name | Ensures all values in a column are <= dataset max | `macros/validation_macros.sql` |

## Environment Variables

| Variable | Required | Default | Description | Used In |
|----------|----------|---------|-------------|---------|
| `DUCKDB_PATH` | No | `dev.duckdb` | DuckDB database path | dbt `profiles.yml` |
| `SNOWFLAKE_ACCOUNT` | For Snowflake | — | Snowflake account identifier | dbt `profiles.yml`, `ingest_raw.py` |
| `SNOWFLAKE_USER` | For Snowflake | — | Snowflake user | dbt `profiles.yml`, `ingest_raw.py` |
| `SNOWFLAKE_PASSWORD` | For Snowflake | — | Snowflake password | dbt `profiles.yml`, `ingest_raw.py` |
| `SNOWFLAKE_ROLE` | For Snowflake | `TRANSFORMER` | Snowflake role | dbt `profiles.yml` |
| `SNOWFLAKE_DATABASE` | For Snowflake | `ANALYTICS` | Snowflake database | dbt `profiles.yml` |
| `SNOWFLAKE_WAREHOUSE` | For Snowflake | `DBT_WH` | Snowflake warehouse | dbt `profiles.yml` |
| `SLACK_WEBHOOK_URL` | No | — | Slack webhook for alerts | `dbt_cosmos_dag.py` |
| `INGEST_MIN_ROWS` | No | `1` | Minimum row count guard for ingest | `ingest_raw.py` |
| `DBT_PROFILE` | No | `duckdb` | Profile name for Airflow DAG | `dbt_cosmos_dag.py` |
| `DBT_TARGET` | No | `dev` | Target name for Airflow DAG | `dbt_cosmos_dag.py` |
| `AIRFLOW_UID` | No | `50000` | Airflow container user ID | `airflow/.env.example` |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Yes | — | Airflow webserver secret | Airflow config |
| `AIRFLOW__CORE__FERNET_KEY` | Yes | — | Airflow connection encryption | Airflow config |
| `_AIRFLOW_DB_MIGRATE` | No | `true` | Auto-run DB migrations | Airflow startup |

## Adding or Upgrading Dependencies

### Requirements Files

The project uses a two-file dependency management pattern:

| File | Role | Format |
|------|------|--------|
| `requirements-dev.in` | **Source of truth** | Loose specifiers (e.g., `dbt-core>=1.8`) |
| `requirements-dev.txt` | **Lockfile** | Pinned exact versions for reproducibility |

### Add a New Dependency

1. Add to `requirements-dev.in`:
   ```
   dbt-utils>=1.0
   ```
2. Regenerate lockfile:
   ```bash
   pip-compile requirements-dev.in --output-file requirements-dev.txt
   ```

### Upgrade All Dependencies

```bash
pip-compile --upgrade requirements-dev.in --output-file requirements-dev.txt
```

### dbt Packages

Edit `dbt_project/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.1.0"
  - package: dbt-labs/codegen
    version: ">=0.12.0"
  - package: calogica/dbt_expectations
    version: ">=0.10.0"
```

Then run:

```bash
make dbt-deps
```

## Code Formatting Standards

### Python

- **Formatter**: `ruff format` (configured in `pyproject.toml`).
- **Linter**: `ruff check` (configured in `pyproject.toml`).
- **Type checker**: `mypy` (configured in `pyproject.toml`).
- **Auto-fix on commit**: Pre-commit hook runs `ruff --fix`.

### SQL

- **Linter**: `sqlfluff` (configured in `pyproject.toml`).
- **Dialect**: `duckdb` (compatible with Snowflake subset).
- **Style guide**: Standard SQLFluff rules with dbt templater.

### YAML/JSON

- No auto-formatter configured. Manual indentation: 2 spaces for YAML.

## Model Naming Conventions

| Prefix | Layer | Materialization | Schema |
|--------|-------|----------------|--------|
| `stg_` | Staging | View | `staging` |
| `int_` | Intermediate | Table | `intermediate` |
| `dim_` | Dimension (mart) | Table / Snapshot-filtered | `marts` |
| `fct_` | Fact (mart) | Table / Incremental | `marts` |

### Column Naming

- **Snake case**: `customer_id`, `total_revenue`, `invoice_date_only`.
- **Surrogate keys**: Named after the table + `_id` (e.g., `customer_id`, `order_item_id`).
- **Date fields**: Suffixed with `_date` or `_at` (e.g., `invoice_date`, `dbt_valid_from`).
- **Flags**: `is_` prefix (e.g., `is_return`).

## Project Architecture Overview

```
project-root/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml              # CI pipeline (PR checks)
│   │   ├── cd.yml              # CD pipeline (prod deploy)
│   │   └── docs.yml            # Docs generation + GitHub Pages
│   ├── dependabot.yml          # Auto-dependency PRs
│   └── PULL_REQUEST_TEMPLATE.md # PR template
├── .pre-commit-config.yaml     # Pre-commit hooks
├── .gitignore                  # Git ignore rules
├── .secrets.baseline           # detect-secrets baseline
├── Makefile                    # All development commands
├── pyproject.toml              # Python project config + tool configs
├── requirements-dev.in         # Loose dependency specifiers
├── requirements-dev.txt        # Pinned dependency lockfile
├── README.md                   # Project overview
├── ARCHITECTURE.md             # Architecture documentation
├── DEVELOPMENT.md              # Development guide (this file)
├── dbt_project/
│   ├── dbt_project.yml         # dbt project config
│   ├── profiles.yml            # Connection profiles (DuckDB + Snowflake)
│   ├── packages.yml            # dbt package dependencies
│   ├── models/
│   │   ├── sources.yml         # Source definitions + freshness
│   │   ├── schema.yml          # Model schema tests
│   │   ├── exposures.yml       # BI tool exposure definitions
│   │   ├── staging/
│   │   │   └── stg_online_retail.sql
│   │   ├── intermediate/
│   │   │   ├── int_customer_metrics.sql
│   │   │   ├── int_product_metrics.sql
│   │   │   └── int_orders.sql
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── dim_dates.sql
│   │       └── fct_orders.sql
│   ├── snapshots/
│   │   ├── dim_customers_snapshot.sql
│   │   └── dim_products_snapshot.sql
│   ├── macros/
│   │   ├── revenue_calculations.sql
│   │   ├── cross_database.sql
│   │   └── validation_macros.sql
│   └── tests/
│       ├── assert_no_high_value_customers_with_negative_revenue.sql
│       └── assert_fct_orders_row_count_matches_staging.sql
├── scripts/
│   ├── ingest_raw.py            # CSV → database loader
│   ├── ci_setup.py              # Synthetic data generator
│   ├── backup.sh                # Backup script
│   └── bootstrap_env.sh         # Environment setup script
├── tests/
│   └── test_scripts.py          # Python unit tests
├── airflow/
│   ├── docker-compose.yaml      # Airflow services
│   ├── Dockerfile               # Airflow custom image
│   ├── dags/
│   │   └── dbt_cosmos_dag.py    # Airflow DAG definition
│   └── .env.example             # Airflow environment template
└── data/
    └── (gitignored)             # CSV data files live here
```
