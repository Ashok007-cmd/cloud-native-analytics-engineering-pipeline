<!-- generated-by: gsd-doc-writer -->
# dbt Project: `online_retail_pipeline`

This document covers the dbt-specific architecture, conventions, and operations for the Cloud-Native Analytics Engineering Pipeline. The dbt project lives at `dbt_project/` and transforms raw Online Retail II transaction data into a Kimball-style star schema.

---

## Architecture Overview

The dbt project implements a **3-tier + snapshot** medallion architecture:

```
┌──────────────────────────────────────────────────────────────┐
│                        RAW LAYER                              │
│          DuckDB / Snowflake schema: raw                        │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ raw.online_retail                                        │  │
│  │ (loaded by scripts/ingest_raw.py, not by dbt)            │  │
│  └─────────────────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────────┤
│                    STAGING LAYER (Views)                       │
│              DuckDB / Snowflake schema: staging                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ stg_online_retail (view)                                 │  │
│  │ ─ Cleanse, cast, dedup, surrogate key, is_return flag     │  │
│  └─────────────────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────────┤
│                   INTERMEDIATE LAYER (Tables)                  │
│            DuckDB / Snowflake schema: intermediate             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
│  │int_orders│  │int_cust..│  │int_prod..│                    │
│  │(orders)  │  │(RFM      │  │(price    │                    │
│  │          │  │ metrics) │  │ stats)   │                    │
│  └──────────┘  └──────────┘  └──────────┘                    │
├──────────────────────────────────────────────────────────────┤
│              SNAPSHOT LAYER (SCD Type 2, check strategy)      │
│             DuckDB / Snowflake schema: analytics               │
│  ┌────────────────────────┐  ┌────────────────────────────┐  │
│  │ dim_customers_snapshot  │  │ dim_products_snapshot      │  │
│  │ SCD2 on segment, orders,│  │ SCD2 on description,       │  │
│  │ revenue changes         │  │ min/max price changes     │  │
│  └────────────────────────┘  └────────────────────────────┘  │
├──────────────────────────────────────────────────────────────┤
│                    MARTS LAYER (Tables)                        │
│              DuckDB / Snowflake schema: marts                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │dim_cust..│  │dim_prod..│  │dim_dates │  │fct_orders│     │
│  │(current  │  │(current  │  │(calendar │  │(incre-   │     │
│  │ state)   │  │ state)   │  │ spine)   │  │ mental)  │     │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
└──────────────────────────────────────────────────────────────┘
```

### Key Design Principles

- **Separation of concerns**: Each layer has a single responsibility. Staging cleanses. Intermediate computes reusable metrics. Snapshots track history. Marts serve BI tools.
- **Current-state dimensions** (`dim_customers`, `dim_products`) are views over snapshots filtered by `dbt_valid_to IS NULL`, providing SCD2-unaware consumers with simple point-in-time current data.
- The `fct_orders` fact table joins to current snapshot rows (`dbt_valid_to IS NULL`) to avoid fan-out from SCD2 multi-row dimensions.
- All shells, macros, and tests are designed to work identically on DuckDB (dev) and Snowflake (prod).

---

## Materialization Strategies

| Layer | Schema | Materialization | Models | Rationale |
|-------|--------|---------------|--------|-----------|
| `models/staging/` | `staging` | `view` | `stg_online_retail` | Views consume no storage, always reflect source data, and are fast to recreate. |
| `models/intermediate/` | `intermediate` | `table` | `int_orders`, `int_customer_metrics`, `int_product_metrics` | Tables improve downstream query performance. These are consumed by multiple downstream models (snapshots + mart views). |
| `models/marts/` | `marts` | `table` | `dim_customers`, `dim_products`, `dim_dates` | Materialized for BI tool query speed. `dim_customers` and `dim_products` use `cluster_by` for partition pruning. |
| `models/marts/` | `marts` | `incremental` | `fct_orders` | Only processes new/changed rows since last run. Uses 3-day lookback window for late-arriving data. `unique_key: order_item_id.` |
| `snapshots/` | `analytics` | `snapshot` (SCD2) | `dim_customers_snapshot`, `dim_products_snapshot` | `strategy: check` tracks historical changes on selected columns. |

### Materialization Configuration Details

**Staging** (`dbt_project.yml`):
```yaml
staging:
  +materialized: view
  +schema: staging
```

**Intermediate** (`dbt_project.yml`):
```yaml
intermediate:
  +materialized: table
  +schema: intermediate
```

**Marts** (`dbt_project.yml`):
```yaml
marts:
  +materialized: table
  +schema: marts
```

**Incremental logic** (`fct_orders.sql`):
```sql
{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='sync_all_columns',
    cluster_by='date_key'
) }}
```

The 3-day lookback window in `fct_orders`:
```sql
{% if is_incremental() %}
WHERE
    staging.invoice_date >= (
        SELECT COALESCE(MAX(invoice_date), CAST('1900-01-01' AS TIMESTAMP))
        FROM {{ this }}
    ) - INTERVAL '3 days'
{% endif %}
```

DuckDB-specific deduplication post-hook:
```sql
{% if is_incremental() and target.type == 'duckdb' %}
    DELETE FROM {{ this }} AS t
    USING (
        SELECT order_item_id, MAX(invoice_date) AS max_date
        FROM {{ this }}
        GROUP BY order_item_id
        HAVING COUNT(*) > 1
    ) AS dup
    WHERE t.order_item_id = dup.order_item_id
      AND t.invoice_date < dup.max_date;
{% endif %}
```

---

## Model Reference

### Staging

**`stg_online_retail`** (`models/staging/stg_online_retail.sql`)
- Materialized as a `view` in schema `staging`.
- Source: `{{ source('raw', 'online_retail') }}`
- Cleansing steps:
  1. Trim whitespace from all string columns.
  2. `TRY_CAST` numeric and timestamp columns (safe casting).
  3. Coalesce null/empty `customer_id` to `__missing`.
  4. Filter out records with null/empty `invoice_no`, `stock_code`, `invoice_date`, `unit_price`.
  5. Reject rows with `unit_price <= 0`.
  6. Deduplicate by `(invoice_no, stock_code, invoice_date, customer_id)` using `ROW_NUMBER()`.
  7. Generate surrogate key `order_item_id` via `dbt_utils.generate_surrogate_key`.
  8. Derive `is_return` boolean flag (`quantity < 0`).
  9. Derive `invoice_date_only` (DATE truncation for join performance).

### Intermediate

**`int_orders`** (`models/intermediate/int_orders.sql`)
- Grain: one row per `(invoice_no, customer_id)` pair.
- Aggregates: `line_item_count`, `total_quantity`, `forward_item_count`, `total_order_value`, `forward_order_value`, `has_returns`, `unique_items`.

**`int_customer_metrics`** (`models/intermediate/int_customer_metrics.sql`)
- Grain: one row per `customer_id`.
- Aggregates: `total_orders`, `total_items`, `total_quantity`, `total_revenue`, `gross_revenue`, `return_count`, `first_order_date`, `last_order_date`, `recency_days`.
- Segmentation via `CASE`:
  - `customer_id == '__missing'` → `'unknown'`
  - `total_orders >= rfm_high_value_min_orders AND gross_revenue >= rfm_high_value_min_revenue` → `'high_value'`
  - `total_orders >= rfm_medium_value_min_orders` → `'medium_value'`
  - else → `'low_value'`
- Uses `CROSS JOIN` with a scalar `MAX(invoice_date)` CTE for efficient recency calculation.

**`int_product_metrics`** (`models/intermediate/int_product_metrics.sql`)
- Grain: one row per `stock_code`.
- Resolves product descriptions by picking the most-frequent description per `stock_code` using `FIRST_VALUE`.
- Aggregates: `order_count`, `total_quantity_sold`, `total_revenue`, `gross_revenue`, `return_count`, `unique_customers`, `min_price`, `max_price`, `avg_price`.
- Surrogate key `product_key` via `dbt_utils.generate_surrogate_key`.

### Marts — Dimensions

**`dim_dates`** (`models/marts/dim_dates.sql`)
- Materialized as `table` in schema `marts`.
- Date spine covering `MIN(invoice_date) - 7 days` to `MAX(invoice_date) + 7 days` with static fallback `2009-12-01` to `2012-01-01`.
- Attributes: `date_key` (YYYYMMDD integer), `full_date`, `year`, `month`, `day`, `quarter`, `day_of_year`, `week_of_year`, `day_name`, `month_name`, `is_weekend`, `is_weekday`.
- Uses `dbt.date_spine` for generation.
- Handles both Snowflake and DuckDB dialects via `target.type` conditionals.

**`dim_customers`** (`models/marts/dim_customers.sql`)
- Current-state view over `dim_customers_snapshot` (filtered `WHERE dbt_valid_to IS NULL`).
- Cluster by `customer_segment`.

**`dim_products`** (`models/marts/dim_products.sql`)
- Current-state view over `dim_products_snapshot` (filtered `WHERE dbt_valid_to IS NULL`).
- Cluster by `stock_code`.

### Marts — Fact

**`fct_orders`** (`models/marts/fct_orders.sql`)
- Materialized as `incremental` in schema `marts`.
- Grain: one row per order line item (matches `stg_online_retail` grain).
- Joins dimensions point-in-time via `dbt_valid_from`/`dbt_valid_to` ranges (not current-only) to correctly attribute historical orders to the dimension version active at order time.
- Unknown dimension keys coalesced: `customer_key = '-1'`, `product_key = '-1'`, `date_key = 19000101`.
- Snowflake production: `cluster_by=['date_key', 'customer_key']` in config, active for Snowflake builds (DuckDB ignores it). Weekly reclustering backstop via `scripts/snowflake_bootstrap.sql`'s `recluster_fct_orders` task.
- Includes a defense-in-depth dedup post-hook for DuckDB incremental runs (no-op under the adapter's default `delete+insert` strategy — see in-model comment).

### Snapshots

**`dim_customers_snapshot`** (`snapshots/dim_customers_snapshot.sql`)
- `strategy: check` on columns `['customer_segment', 'total_orders', 'total_revenue']`.
- `unique_key: customer_id`.
- Sources: all distinct customer IDs from `stg_online_retail` LEFT JOIN `int_customer_metrics`.

**`dim_products_snapshot`** (`snapshots/dim_products_snapshot.sql`)
- `strategy: check` on columns `['description', 'min_price', 'max_price']`.
- `unique_key: stock_code`.
- Sources: `int_product_metrics`.

---

## Custom Macros Reference

### `calculate_gross_revenue` (`macros/revenue_calculations.sql`)

```sql
{% macro calculate_gross_revenue(quantity_col='quantity', price_col='unit_price', is_return_col='is_return') %}
    SUM(CASE WHEN NOT {{ is_return_col }} THEN {{ quantity_col }} * {{ price_col }} ELSE 0 END)
{% endmacro %}
```

- **Purpose**: Computes gross revenue excluding returns.
- **Usage**: Called in `int_customer_metrics` and `int_product_metrics`:
  ```sql
  {{ calculate_gross_revenue() }} AS gross_revenue
  ```
- **Cross-database**: SQL-only, no dialect dependencies.

### `dayofweek_expression` (`macros/dayofweek_expression.sql`)

```sql
{% macro dayofweek_expression() %}
{% if target.type == 'snowflake' %} EXTRACT(dayofweekiso FROM date_day)
{% else %} EXTRACT(isodow FROM date_day) {% endif %}
{% endmacro %}
```

- **Purpose**: Returns a cross-database ISO day-of-week expression (1=Monday through 7=Sunday).
- **Usage**: Called in `dim_dates.sql` to compute `day_name`, `is_weekend`, and `is_weekday`.

### Custom Generic Tests

**`test_positive_value`** (`macros/test_positive_value.sql`):
```sql
{% macro test_positive_value(model, column_name) %}
    SELECT {{ adapter.quote(column_name) }}
    FROM {{ model }}
    WHERE {{ adapter.quote(column_name) }} <= 0
       OR {{ adapter.quote(column_name) }} IS NULL
{% endmacro %}
```
- **Purpose**: Asserts that a column contains only values > 0.
- **Usage**: `- positive_value` on `unit_price`, `order_count`, `avg_price`, etc.

**`test_not_in_future`** (`macros/test_not_in_future.sql`):
```sql
{% macro test_not_in_future(model, column_name) %}
    SELECT {{ adapter.quote(column_name) }}
    FROM {{ model }}
    WHERE {{ adapter.quote(column_name) }} > {{ dbt.current_timestamp() }}
{% endmacro %}
```
- **Purpose**: Asserts that a timestamp column contains no future dates.
- **Usage**: `- not_in_future` on `invoice_date` in both `stg_online_retail` and `fct_orders`.

---

## Test Catalog

The project has **108+ automated tests** spanning three categories:

### 1. Schema Tests (declared in `models/schema.yml`)

| Test Type | Column(s) | What It Validates |
|-----------|-----------|-------------------|
| `unique` | `order_item_id`, `customer_key`, `product_key`, `date_key`, `full_date`, `customer_id`, `stock_code` | No duplicate values in the column |
| `unique (where: dbt_valid_to IS NULL)` | `customer_id`, `stock_code`, `customer_key`, `product_key` on snapshot models | No duplicates among currently-valid snapshot rows |
| `not_null` | All key columns, all metric columns | No NULL values |
| `accepted_values` | `is_return`, `customer_segment`, `is_weekend`, `is_weekday` | Values are within an allowed set |
| `dbt_utils.expression_is_true` | `quantity != 0` in `stg_online_retail` | Ensures no zero-quantity rows (after negative filter) |
| `dbt_utils.unique_combination_of_columns` | `(invoice_no, customer_id)` in `int_orders` | Ensures the grain is unique |
| `positive_value` (custom) | `unit_price`, `order_count`, `total_quantity_sold`, `min_price`, `max_price`, `avg_price`, `line_item_count`, `unique_items` | All values are strictly > 0 |
| `not_in_future` (custom) | `invoice_date` in `stg_online_retail` and `fct_orders` | No future dates |
| `relationships` | `customer_key` → `dim_customers_snapshot`, `product_key` → `dim_products_snapshot`, `date_key` → `dim_dates` | Referential integrity |

### 2. Singular Tests (`dbt_project/tests/`)

| Test File | What It Validates |
|-----------|-------------------|
| `assert_no_high_value_customers_with_negative_revenue.sql` | No customer classified as `high_value` has `gross_revenue <= 0`. This is a data integrity invariant: high_value requires both order count AND revenue minimums. |
| `assert_fct_orders_row_count_matches_staging.sql` | Row count difference between `fct_orders` and `stg_online_retail` is ≤ 0.1%. Detects join fan-out or data loss. |

### 3. Source Freshness Tests (`models/sources.yml`)

| Source | Warn After | Error After | Loaded At Field |
|--------|-----------|-------------|-----------------|
| `raw.online_retail` | 24 hours | 48 hours | `_ingested_at` |

### Running Tests

```bash
# Run all dbt tests
make test

# Run a specific test
cd dbt_project && dbt test --select test_name:assert_fct_orders_row_count_matches_staging

# Run tests for a specific model
cd dbt_project && dbt test --select stg_online_retail

# Run tests with source freshness check
cd dbt_project && dbt build --profiles-dir .
```

---

## Environment Variables

The dbt project uses the following environment variables:

| Variable | Required | Default | Used In | Description |
|----------|----------|---------|---------|-------------|
| `SNOWFLAKE_ACCOUNT` | For Snowflake prod | — | `profiles.yml` | Snowflake account identifier |
| `SNOWFLAKE_USER` | For Snowflake prod | — | `profiles.yml` | Snowflake login user |
| `SNOWFLAKE_PASSWORD` | For Snowflake prod | — | `profiles.yml` | Snowflake password |
| `SNOWFLAKE_ROLE` | No | `DBT_ROLE` | `profiles.yml` | Snowflake role for dbt sessions |
| `SNOWFLAKE_DATABASE` | No | `ONLINE_RETAIL_DB` | `profiles.yml` | Snowflake database name |
| `SNOWFLAKE_WAREHOUSE` | No | `ELT_WH` | `profiles.yml` | Snowflake warehouse name |
| `DUCKDB_PATH` | No | `dev.duckdb` | `profiles.yml` | Path to DuckDB database file |
| `DBT_PROFILE` | In Airflow | `duckdb` | `dbt_cosmos_dag.py` | Profile name for Airflow Cosmos |
| `DBT_TARGET` | In Airflow | `dev` | `dbt_cosmos_dag.py` | Target name for Airflow Cosmos |

These are set via the `env_var()` Jinja function in `profiles.yml`. In Airflow/Docker, they come from the `.env` file. In CI, they're injected via `env` blocks in GitHub Actions.

---

## dbt Packages

Defined in `packages.yml`:

| Package | Version | Purpose |
|---------|---------|---------|
| `dbt-labs/dbt_utils` | `>=1.1.0, <1.3.0` | `generate_surrogate_key`, `expression_is_true`, `unique_combination_of_columns`, `date_spine` |
| `dbt-labs/codegen` | `>=0.12.0, <0.14.0` | Code generation utilities (development only) |
| `calogica/dbt_expectations` | `>=0.10.0, <0.11.0` | Additional test macros |

---

## dbt Project Variables

Defined in `dbt_project.yml`:

```yaml
vars:
  surrogate_key_treat_nulls_as: ''
  rfm_high_value_min_orders: 10
  rfm_high_value_min_revenue: 1000
  rfm_medium_value_min_orders: 3
```

Override per environment:
```bash
# CLI override
cd dbt_project && dbt build --vars '{"rfm_high_value_min_orders": 5, "rfm_high_value_min_revenue": 500}'
```

---

## How to Add a New Model

### 1. Create the SQL file

```bash
# Example: a new intermediate model
touch dbt_project/models/intermediate/int_daily_metrics.sql
```

### 2. Write the model

```sql
{{
    config(
        materialized='table'  -- or 'view' for staging, 'incremental' for large fact tables
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_online_retail') }}
)

SELECT
    invoice_date_only AS date_day,
    COUNT(DISTINCT invoice_no) AS order_count,
    SUM(quantity * unit_price) AS revenue
FROM source
GROUP BY invoice_date_only
```

### 3. Add columns to `schema.yml`

Add a new entry under `models:` with column-level tests:

```yaml
- name: int_daily_metrics
  description: "Daily sales aggregation for reporting"
  columns:
    - name: date_day
      tests: [unique, not_null]
    - name: order_count
      tests: [not_null, positive_value]
    - name: revenue
      tests: [not_null]
```

### 4. Wire into downstream models (if applicable)

If this is a new intermediate model that feeds into marts, add `ref()` calls in the downstream models.

### 5. Run and verify

```bash
make build-models
```

---

## How to Add a New Test

### Generic Test (reusable across columns/models)

Add a custom macro in `macros/`:

```sql
{% macro test_my_custom_check(model, column_name) %}
    SELECT {{ adapter.quote(column_name) }}
    FROM {{ model }}
    WHERE NOT condition
{% endmacro %}
```

Use it in `schema.yml`:

```yaml
tests:
  - my_custom_check
```

### Singular Test (model-specific logic)

Add a `.sql` file in `dbt_project/tests/`:

```sql
-- assert_revenue_is_positive.sql
SELECT order_item_id, revenue
FROM {{ ref('fct_orders') }}
WHERE revenue < 0 AND is_return = FALSE
```

### Add Python unit tests

Add test functions in `tests/test_scripts.py`:

```python
def test_something():
    assert expected == actual
```

---

## Common Troubleshooting

### `dbt: command not found`
Ensure dbt is installed:
```bash
pip install dbt-duckdb dbt-snowflake
```
Or use the dedicated virtual environment:
```bash
source dbt_venv/bin/activate
```

### `compilation error: model not found`
- Missing `ref()` or misspelled model name.
- Run `dbt ls` to list available models:
  ```bash
  cd dbt_project && dbt ls --profiles-dir .
  ```

### `incremental model is empty`
- First run on a new database: incremental models act like `table` materialization on first run.
- Ensure the source is populated:
  ```bash
  python scripts/ingest_raw.py
  ```

### `duplicate rows in fct_orders` (DuckDB)
DuckDB's `MERGE` with `unique_key` appends rather than upserts. The post-hook in `fct_orders.sql` deduplicates:
```sql
DELETE FROM {{ this }} ... WHERE COUNT(*) > 1
```
If you see duplicates, run a full `dbt build` (not incremental) or run the dedup post-hook manually.

### `Snapshots not capturing changes`
- Snapshot `check_cols` only tracks the columns listed. If a tracked column doesn't change, no new row is created.
- Check the snapshot strategy: `dim_customers_snapshot` tracks `customer_segment`, `total_orders`, `total_revenue`. If only `return_count` changes, it won't trigger a new row.

### `WAL file growing large` (DuckDB)
DuckDB appends changes to a WAL file. Periodic `dbt build` runs are normal. If the WAL grows excessively (>1GB), compact the database:
```sql
-- In DuckDB
CHECKPOINT;
```

---

## dbt Commands Reference

All commands assume `cd dbt_project && dbt ... --profiles-dir .`

| Command | Purpose |
|---------|---------|
| `dbt deps` | Install packages from `packages.yml` |
| `dbt build` | Run models + tests in dependency order |
| `dbt run` | Run models only (skip tests) |
| `dbt test` | Run tests only (skip model builds) |
| `dbt snapshot` | Run snapshot definitions only |
| `dbt source freshness` | Check source freshness thresholds |
| `dbt docs generate` | Generate documentation catalog |
| `dbt docs serve` | Serve documentation on localhost:8080 |
| `dbt ls` | List all project resources |
| `dbt clean` | Remove `target/`, `dbt_packages/`, databases |
| `dbt debug` | Validate connection and project config |

---

## dbt Project Directory Structure

```
dbt_project/
├── profiles.yml            # Connection profiles (DuckDB, Snowflake)
├── dbt_project.yml         # Project config, materializations, vars
├── packages.yml            # dbt package dependencies
├── models/
│   ├── schema.yml          # Column-level tests
│   ├── sources.yml         # Source definitions + freshness
│   ├── exposures.yml       # Downstream BI dashboard metadata
│   ├── staging/
│   │   └── stg_online_retail.sql
│   ├── intermediate/
│   │   ├── int_orders.sql
│   │   ├── int_customer_metrics.sql
│   │   └── int_product_metrics.sql
│   └── marts/
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── dim_dates.sql
│       └── fct_orders.sql
├── snapshots/
│   ├── dim_customers_snapshot.sql
│   └── dim_products_snapshot.sql
├── macros/
│   ├── revenue_calculations.sql
│   ├── test_positive_value.sql
│   ├── test_not_in_future.sql
│   └── dayofweek_expression.sql
├── tests/
│   ├── assert_no_high_value_customers_with_negative_revenue.sql
│   └── assert_fct_orders_row_count_matches_staging.sql
├── docs/                   # dbt docs directory (auto-generated)
├── target/                 # dbt build artifacts (git-ignored)
└── dbt_packages/           # dbt package installs (git-ignored)
```
