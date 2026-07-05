# Cloud-Native Analytics Engineering Pipeline — Research Report

**Generated:** 2026-06-28  
**Codebase:** Cloud-Native Analytics Engineering Pipeline (DuckDB + Snowflake + dbt + Airflow + Cosmos)  
**Status:** This report is an **incremental analysis** — it builds on prior work documented in `ANALYSIS_REPORT.md`, `IMPROVEMENT_PLAN.md`, and `REVIEW.md`, identifies what has already been fixed, and recommends **what remains to be addressed** with new findings.

---

## Executive Summary

The project has undergone substantial remediation across all 7 dimensions. Prior reports identified **4 critical bugs**, **7 security findings**, **18 improvement opportunities**, and **107+ improvement items** — the vast majority have been applied.

- **137+ passing dbt tests** (up from ~107 baseline, with FK relationship tests now actually executing)
- **SCD2 snapshots fixed** — `recency_days` and `avg_price` removed from `check_cols`, preventing unbounded history growth
- **CI/CD hardened** — SHA-pinned actions, `detect-secrets` scanning, post-ingest row count verification, source freshness checks
- **DAG fixed** — module-level pool creation removed, `datetime.utcnow()` deprecated call replaced, `except: pass` eliminated
- **Security posture improved** — Fernet key rotation documented, webserver bound to localhost, `.env` excluded from git, `SECURITY.md` created, `.secrets.baseline` generated
- **Performance optimised** — `int_customer_metrics` single-scan pattern, `read_csv(all_varchar=true)` for deterministic ingest
- **Developer practices upgraded** — `pyproject.toml` with ruff/pytest/mypy config, Python tests, pre-commit hooks, PR templates, CONTRIBUTING.md

This report focuses on **Phase 2 improvements** — the next tier of maturity that takes the pipeline from "well-engineered demo" to "production-hardened system."

---

## Dimension 1: Architecture

### Current State (Post-Fix)

| File | Issue (Past) | Resolution | Status |
|------|-------------|-----------|--------|
| `fct_orders.sql` | SCD2 snapshot joins missing `WHERE dbt_valid_to IS NULL` → duplicate rows | Filter added | ✅ |
| `schema.yml` | FK tests referenced non-existent `dim_customers`/`dim_products` | Changed to `dim_customers_snapshot`/`dim_products_snapshot` | ✅ |
| `dim_customers_snapshot.sql` | `recency_days` in `check_cols` → daily snapshot growth explosion | Removed; only `customer_segment` tracked | ✅ |
| `dim_products_snapshot.sql` | `avg_price` in `check_cols` → excessive snapshot rows on each order | Changed to `min_price`/`max_price` | ✅ |
| `stg_online_retail.sql` | `SELECT * EXCLUDE (row_num)` → DuckDB-only syntax | Replaced with explicit column list | ✅ |
| `fct_orders.sql` | Missing `country` column → no geographic analytics | Added `staging.country` | ✅ |
| `sources.yml` | `loaded_at_field: invoice_date` → freshness always errors on historical data | Changed to `_ingested_at` | ✅ |
| `fct_orders.sql` | Incremental `>` boundary → possible missing rows on same-timestamp invoices | Now uses `>=` with 3-day lookback | ✅ |
| `dim_dates.sql` | Non-standard `CEIL(DOY/7.0)` week calculation | Changed to `WEEKOFYEAR` (Snowflake) / `EXTRACT('week')` (DuckDB) | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **A-01** | **Intermediate layer is flat** — only 2 intermediate models (`int_customer_metrics`, `int_product_metrics`). No separate return/refund model or order-header intermediate. An order-header intermediate (`int_orders`) would enable order-level analytics without coupling fact-row granularity. | Create `int_orders.sql`: aggregate `stg_online_retail` at invoice level (order date, item count, total value, return flag). This enables BI queries like "average order value by month" without scanning all 1M fact rows. | Medium | 2–3h |
| **A-02** | **dim_dates has no holiday/event calendar** — no holiday flags, no fiscal periods, no seasonal markers. Retail analytics typically need holiday comparisons (Christmas, Black Friday). | Add fiscal year/month/week columns, holiday flags, and retail season markers (Christmas, Summer Sale, etc.). Could use a seed CSV or populate via a macro with date logic. | Low | 1–2h |
| **A-03** | **No dbt-exposures defined** — downstream BI tool usage is undocumented. Without `exposures.yml`, dbt docs can't show which dashboards/reports depend on which models. | Create `exposures.yml` documenting BI tool usage (e.g., "Monthly Revenue Dashboard", "Customer 360 Report") with explicit `depends_on:` model references. | Medium | 30min |
| **A-04** | **fct_orders lacks invoice-level grain** — currently line-item grain. Adding an `invoice_no` degenerate dimension + `int_orders` grain enables order-level analytics (AOV, order frequency) that line-item grain doesn't support cleanly. | The `invoice_no` is already in `fct_orders` as a degenerate dimension (good pattern). Add `int_orders` to leverage it for order-level aggregations. | Medium | 2–3h |
| **A-05** | **Source freshness thresholds may need tuning** — currently warn=24h, error=48h. For the static dataset (2009–2011) this is irrelevant, but for production with daily CSV drops: if the source file arrives at 9am but the Airflow DAG runs at midnight, freshness alerts could fire every night. | Set thresholds aligned to the actual delivery SLA: if CSVs arrive by 10am daily, set `warn_after: {count: 12, period: hour}` and `error_after: {count: 24, period: hour}`. Document the SLA in `sources.yml` comments. | Low | 5min |

---

## Dimension 2: Performance Optimisation

### Current State (Post-Fix)

| File | Issue (Past) | Resolution | Status |
|------|-------------|-----------|--------|
| `int_customer_metrics.sql` | Two scans of staging (separate CTE for max_date + aggregation) | Single scan with `MAX() OVER ()` window function | ✅ |
| `scripts/ingest_raw.py` | `read_csv_auto()` → slow column-type inference; no `_ingested_at` | Changed to `read_csv(..., all_varchar=true)`, added `_ingested_at` | ✅ |
| `scripts/backup.sh` | Multi-stream gzip → most tools only read first stream | Changed to `tar -czf` | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **P-01** | **fct_orders incremental without unique_key enforcement** — `unique_key='order_item_id'` is set but `on_schema_change='sync_all_columns'` doesn't handle deduplication during merge. If a row with the same `order_item_id` arrives with different data, Snowflake's `MERGE` will update it; DuckDB will append duplicate. | Add a dbt post-hook to deduplicate: `{{ config(post_hook="DELETE FROM {{ this }} WHERE (order_item_id, invoice_date) NOT IN (SELECT order_item_id, MAX(invoice_date) FROM {{ this }} GROUP BY order_item_id)") }}`. Or switch to `insert_overwrite` on Snowflake for the fact table. | High | 1–2h |
| **P-02** | **No Snowflake-specific clustering** — the `cluster_by` Jinja comment in `fct_orders.sql` is not automated. Snowflake requires `ALTER TABLE ... CLUSTER BY (...)` outside dbt. | Automate in the CD pipeline: add a `--var run_cluster` flag that executes `ALTER TABLE fct_orders CLUSTER BY (date_key, customer_key)` only on Snowflake targets. Use a dbt `on_schema_change` hook or a dedicated post-deploy script. | Medium | 2h |
| **P-03** | **Intermediate tables use full refresh every run** — `int_customer_metrics` and `int_product_metrics` are `materialized='table'` (full refresh). For 1M rows this is fast, but at 100M+ rows incremental would save significant compute. | Consider incremental materialization for intermediate tables once row counts exceed 50M+. Use `strategy='check'` or `unique_key` with `incremental_strategy='delete+insert'`. Document as a scaling trigger threshold. | Low | 1h |
| **P-04** | **DuckDB single-writer lock contention** — `duckdb_pool` with 1 slot prevents concurrent writes but also prevents parallel dbt model builds on DuckDB. | Document that this is by design for DuckDB's MVCC limitations. For Snowflake production, remove the pool constraint and set `threads: 8+` in `profiles.yml`. Consider using `emit_filtered_results_for` for parallel model execution when Snowflake is the target. | Low | 30min |
| **P-05** | **No query performance benchmarking** — no `EXPLAIN ANALYZE` benchmarking, no query timing in dbt logs. Without baselines, it's impossible to detect regression when models change. | Add a `Makefile` target: `benchmark: dbt run --full-refresh --profiles-dir . --no-populate-cache && dbt docs generate`. Wrap with `time` command. Store baseline timings in a spreadsheet or `.benchmark.json` for regression detection in CI. | Low | 1h |

---

## Dimension 3: Security

### Current State (Post-Fix)

| Item | Resolution | Status |
|------|-----------|--------|
| Fernet key exposure in committed `.env` | `.env` added to `.gitignore`, SECURITY.md documents rotation requirement | ✅ |
| Airflow webserver on `0.0.0.0:8080` | Changed to `127.0.0.1:8080:8080` | ✅ |
| Default `admin/admin` credentials | `.env.example` now includes `AIRFLOW_ADMIN_PASSWORD` with strong password guidance | ✅ |
| No secret scanning in CI | `detect-secrets` step added to CI pipeline | ✅ |
| GitHub Actions not SHA-pinned | `ci.yml` and `docs.yml` now use SHA-pinned actions | ✅ |
| Source CSV in git | `data/*.csv` excluded from git | ✅ |
| No `.secrets.baseline` | Baseline generated with appropriate excludes | ✅ |
| No SECURITY.md | Created with reporting SLA and credential handling docs | ✅ |
| No detect-secrets pre-commit hook | Added to `.pre-commit-config.yaml` | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **S-01** | **Python dependencies not hash-pinned** — `requirements-dev.txt` and `airflow/requirements.txt` use version ranges (`>=`, `<`) without `--hash` locks. A malicious package version within the allowed range would pass CI. | Generate hash-pinned requirements: `pip-compile --generate-hashes requirements.in > requirements.txt`. Use a separate `requirements.in` for loose version specs during development and `requirements.txt` (hash-locked) for CI/CD and Docker. | High | 1h |
| **S-02** | **Docker image uses mutable tag** — `FROM apache/airflow:2.10.4-python3.12` has a comment about pinning to a digest but doesn't actually do it. When `2.10.4-python3.12` is republished, builds become non-reproducible. | Pin to a specific digest: `FROM apache/airflow@sha256:<actual-sha256>`. The comment already shows the `docker inspect` command; run it and replace the tag. | High | 10min |
| **S-03** | **Docker runs as root for `apt-get install`** — the Dockerfile switches to `USER root` then back to `USER airflow`. If git is already in the base image, the entire `apt-get` block is unnecessary and just enlarges the image + attack surface. | Check if git is already present in the base Airflow 2.10.4 image: `docker run apache/airflow:2.10.4-python3.12 which git`. If present, remove the entire `USER root` block. | Medium | 15min |
| **S-04** | **No SBOM generation** — no Software Bill of Materials is generated for the Airflow Docker image or Python dependencies. CVE scanning can't happen without an SBOM. | Add a CI step to generate SBOM with `pip-audit` or `cyclonedx-bom` (for pip) and `trivy image` (for Docker). Fail CI on critical CVEs. | Medium | 1h |
| **S-05** | **Snowflake credentials in Airflow `.env`** — `SNOWFLAKE_PASSWORD` is stored in plaintext in the `.env` file. If the `.env` file leaks, Snowflake credentials are exposed. | For production: use Airflow Connections (encrypted at rest by Fernet key) or a secrets backend (Vault, AWS Secrets Manager). Document in SECURITY.md. For dev: low risk since `.env` is gitignored and local-only. | Medium | 2h |
| **S-06** | **No `pip-audit` in CI** — CI installs dependencies but never scans them for known vulnerabilities. | Add `pip install pip-audit && pip-audit` step to CI. Fail on high/critical known vulnerabilities. | Medium | 30min |
| **S-07** | **No dependabot/renovate configuration** — dependencies are pinned but there's no automated update mechanism. Outdated deps = unpatched CVEs. | Add `.github/dependabot.yml` to check weekly for pip, Docker, and GitHub Actions updates. Set `open-pull-requests-limit: 5`. | Medium | 30min |

---

## Dimension 4: Scalability

### Current State (Post-Fix)

| Item | Resolution | Status |
|------|-----------|--------|
| SCD2 snapshot growth from `recency_days` | Fixed — only `customer_segment` triggers snapshot changes | ✅ |
| SCD2 excessive rows from `avg_price` | Fixed — only `min_price`/`max_price` changes trigger snapshots | ✅ |
| Snowflake clustering documented | Comment added in `fct_orders.sql` explaining `CLUSTER BY (date_key, customer_key)` | ✅ |
| Cosmos `RenderConfig` optimization | Skipped — version-dependent; comment added documenting the rationale | ⚠️ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **SC-01** | **No Snowflake warehouse sizing strategy** — `ELT_WH` is a single warehouse. For production at scale, you need separate warehouses for dbt builds (compute-heavy) vs. BI queries (concurrent but lighter). | Define Snowflake warehouse strategy: (1) `DBT_WH` — X-Small or Small, multi-cluster, auto-suspend 5min; (2) `BI_WH` — Small or Medium, multi-cluster, auto-suspend 1min. Document in `profiles.yml` comments. | Medium | 30min |
| **SC-02** | **SCD2 on `min_price`/`max_price` can still grow** — if a product sells at 20 different prices, every new min or max triggers a snapshot row. For 5K products, this is fine. For 500K products, could be problematic. | Document in the snapshot SQL the growth model: "expects 1-2 snapshot rows per product per year at current change rate." Add a dbt test that monitors `ROW_COUNT / COUNT(DISTINCT stock_code)` ratio to alert if average history depth exceeds a threshold (e.g., 5 rows per product). | Low | 1h |
| **SC-03** | **No partition strategy for Snowflake fact table** — `fct_orders` will grow unboundedly. Without partitioning (Snowflake clustering or Iceberg partitioning), queries on historical ranges scan the full table. | Document a partition strategy: cluster by `date_key` (Snowflake) or use `INSERT_ONLY` incremental with periodic `ALTER TABLE fct_orders CLUSTER BY (date_key, customer_key);` re-clustering. For DuckDB, partitioning is not supported natively but `ORDER BY date_key` in the table creation helps. | High | 1h |
| **SC-04** | **`dbt source freshness` always passes in dev** — `_ingested_at` is `CURRENT_TIMESTAMP`, so freshness checks are meaningless. For production, `_ingested_at` should reflect the actual CSV drop time. | Document a production pattern: when Snowflake receives a CSV via external stage, set `_ingested_at` to the file's `LAST_MODIFIED` timestamp (Snowflake `METADATA$FILE_LAST_MODIFIED`) rather than `CURRENT_TIMESTAMP`. This enables meaningful freshness SLAs. | Low | 1h |
| **SC-05** | **Cosmos `RenderConfig` not used** — DbtDag reads every model file on DAG parse. For 100+ models, this adds significant parse time (Airflow may skip DAGs). | After verifying Cosmos version (>=1.6.0), add `RenderConfig(load_mode=LoadMode.DBT_LS)` to `DbtDag`. This uses `dbt ls` (one subprocess call) instead of loading every model file in Python. | Medium | 30min |

---

## Dimension 5: Code Quality

### Current State (Post-Fix)

| File | Issue | Resolution | Status |
|------|-------|-----------|--------|
| `dbt_cosmos_dag.py` | Module-level `FileNotFoundError` crash | Changed to `logging.warning` | ✅ |
| `dbt_cosmos_dag.py` | Module-level pool creation with `except: pass` | Removed; pool created in init container | ✅ |
| `dbt_cosmos_dag.py` | `datetime.utcnow()` deprecated in 3.12 | Changed to `datetime.now(tz=timezone.utc)` | ✅ |
| `schema.yml` | `dbt_utils.expression_is_true` deprecation warning | Fixed by wrapping in `arguments:` block | ✅ |
| `ci_setup.py` | `DST` resolved at module import time (broke test isolation) | Moved inside `main()` | ✅ |
| `ci_setup.py` | Missing `from __future__ import annotations` | Added | ✅ |
| `stg_online_retail.sql` | ROW_NUMBER never filtered (dedup not applied) | Verified: this was fixed — `row_num = 1` filter added | ✅ |
| `stg_online_retail.sql` | `ORDER BY invoice_no` is no-op in dedup window | Changed to `ORDER BY description, country` | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **CQ-01** | **`fct_orders.sql` uses `>=` for incremental boundary but not `+lookback`** — the current approach `MAX(invoice_date) - INTERVAL '3 days'` is robust but non-standard. dbt natively supports `+lookback` config which is more declarative. | Add `lookback=3` to the incremental config and remove the manual lookback logic. The `config()` block would become: `materialized='incremental', unique_key='order_item_id', on_schema_change='sync_all_columns', lookback=3`. Test on both DuckDB and Snowflake to verify support. | Medium | 1h |
| **CQ-02** | **No SQLFluff severity blocking in CI** — `make lint-sql` runs SQLFluff but CI doesn't fail on rule violations. SQL style drifts over time. | Add `--fatal` flag or use `--processes` to SQLFluff in CI. Or make the lint step fail on any rule violation: `sqlfluff lint dbt_project/models/ dbt_project/snapshots/ --dialect duckdb --fatal` | Medium | 15min |
| **CQ-03** | **`profiles.yml` Snowflake account defaults to empty string** — `account: "{{ env_var('SNOWFLAKE_ACCOUNT', '') }}"`. If env var is missing, dbt fails with a cryptic Snowflake error instead of a clear message. | Remove the default value: `account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"`. This makes dbt fail at compile time with "env_var not found" — much clearer. | Low | 5min |
| **CQ-04** | **No type annotations in `ci_setup.py`** — most functions lack return type hints. | Add `-> str` to `random_customer_id()`, `-> str` to `random_date()`, `-> None` to `main()`. Also type-annotate module-level variables. | Low | 10min |
| **CQ-05** | **`test_not_in_future.sql` uses direct column interpolation** — `WHERE {{ column_name }}` should use `adapter.quote()` for defense-in-depth. | Change to: `WHERE {{ adapter.quote(column_name) }} > {{ dbt.current_timestamp() }}`. Same pattern applies to `test_positive_value.sql`. | Low | 10min |
| **CQ-06** | **`assert_fct_orders_row_count_matches_staging.sql` uses strict float comparison** — `ABS(fact.n::DOUBLE - staging.n::DOUBLE)` may produce floating-point edge cases for very large row counts (>2^53). | Use integer arithmetic: `ABS(fact.n - staging.n) <= CAST(0.001 * staging.n AS BIGINT)`. Avoids FP precision issues at scale. | Low | 5min |

---

## Dimension 6: Developer Practices

### Current State (Post-Fix)

| Item | Resolution | Status |
|------|-----------|--------|
| No `pyproject.toml` | Created with ruff/pytest/mypy config | ✅ |
| No Python tests | `tests/test_scripts.py` with 4 tests | ✅ |
| No pre-commit hooks | Trailing-whitespace, ruff, SQLFluff, detect-secrets, merge-conflict, no-commit-to-branch | ✅ |
| SQLFluff only on models directory | Extended to include `snapshots/` | ✅ |
| No PR template | Created with type-of-change checkboxes and build/lint checklist | ✅ |
| No bug report template | Created | ✅ |
| CONTRIBUTING.md missing Python test section | Added pytest instructions | ✅ |
| `Makefile` `build` target unconditionally re-ingests | Added `build-models` as a standalone target | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **DP-01** | **No Devcontainer configuration** — new contributors must manually set up Python venv, pre-commit, dbt profiles, and Docker. A devcontainer would provide a zero-setup development environment. | Add `.devcontainer/devcontainer.json` with: (1) `python:3.11` base, (2) post-create commands to install pip deps, (3) pre-commit install, (4) Docker-in-Docker for Airflow. Reference in CONTRIBUTING.md. | High | 2h |
| **DP-02** | **No `dbt debug` check in developer setup** — new developers might set up profiles incorrectly and get cryptic dbt errors. | Add `dbt debug --profiles-dir .` to the Makefile as a `setup` target that validates the profile configuration before running models. Reference in CONTRIBUTING.md. | Medium | 15min |
| **DP-03** | **No dbt test documentation generated as CI artifact** — developers can't see test coverage trends over time. | In CI, archive `dbt_project/target/` as a build artifact so developers can download `sources.json`, `tests.json`, etc. to see which tests ran and their results. | Low | 15min |
| **DP-04** | **No Makefile `setup` target** — new developers must manually run `pip install`, `pre-commit install`, `dbt deps`. | Add a `setup` target: `pip install -r requirements-dev.txt && pre-commit install && cd dbt_project && dbt deps`. This reduces friction from 3 steps to 1: `make setup`. | Medium | 5min |
| **DP-05** | **No dbt `--no-populate-cache` in CI** — dbt's cache population (introspecting the database schema) adds parse time without benefit in CI where the database is empty. | Add `--no-populate-cache` to CI dbt commands: `dbt build --profiles-dir . --no-populate-cache --no-partial-parse`. | Low | 5min |
| **DP-06** | **`profiles.yml.template` exists but isn't referenced in setup instructions** — no one uses it. | Either document it in CONTRIBUTING.md as a fallback or remove it to reduce confusion. | Low | 5min |

---

## Dimension 7: Integration Stability

### Current State (Post-Fix)

| Item | Resolution | Status |
|------|-----------|--------|
| `docs.yml` used `dbt run` instead of `dbt build` | Changed to `dbt build` — tests pass before docs publish | ✅ |
| CI installed full Airflow | Changed to `requirements-dev.txt` only | ✅ |
| No post-ingest row count check | Added "Verify ingest row count" step | ✅ |
| Source freshness `|| true` unexplained | Added comment explaining `_ingested_at` is `CURRENT_TIMESTAMP` | ✅ |
| Unpinned package versions | Upper bounds added to all requirements files | ✅ |
| `dbt_utils.date_spine` deprecated | Changed to `dbt.date_spine` | ✅ |

### Remaining Recommendations

| ID | Finding | Recommendation | Priority | Effort |
|----|---------|---------------|----------|--------|
| **IS-01** | **No Snowflake CI job** — CI only tests against DuckDB. Snowflake-specific SQL (Snowflake `WEEKOFYEAR`, clustering) is not validated before production deploy. Snowflake-only failures show up during CD on main branch. | Add a `snowflake-ci` job to `cd.yml` (or a nightly scheduled workflow) that runs `dbt build --profiles-dir . --profile snowflake --target prod --select state:modified` on a Snowflake dev environment or Snowflake free trial. | High | 3–4h |
| **IS-02** | **Airflow connection retry not configured** — `dbt_cosmos_dag.py` has `retries: 2` with exponential backoff, which retries Cosmos task execution. But Snowflake connections may fail with transient errors (connection pool exhausted, warehouse resume delay) that retry won't help if the dbt profile doesn't have connection retry settings. | Add Snowflake `client_session_keep_alive: true` and `retry_on_database_errors: true` to the Snowflake profile. Set Snowflake warehouse auto-resume. Add a connection timeout: `query_tag: dbt_build_{{ model_name }}` for tracing. | Medium | 30min |
| **IS-03** | **Docker compose health checks might cause race conditions** — Airflow services depend on `postgres: condition: service_healthy` but also have their own health checks. If Airflow starts before PostgreSQL is ready for connections, the first DB migration may fail. | Add a `restart: on-failure` to `airflow-init` and increase the initial delay on the Airflow health check: `interval: 10s, retries: 10, start_period: 30s`. | Medium | 10min |
| **IS-04** | **No Airflow connection/DAG import error alerting** — if the DAG fails to parse (e.g., Cosmos API change, missing profile), Airflow silently skips it. There's no alert for DAG disappearing from the UI. | Add a monitoring DAG (or use Airflow's built-in `DagFileProcessorManager` alerts) that periodically checks that `online_retail_elt` DAG is present and unpaused. Use `airflow dags list` or the REST API in a simple health-check script. | Medium | 1h |
| **IS-05** | **No GitHub Actions cache for dbt packages** — every CI run runs `dbt deps`, downloading dbt packages from scratch (~15s per run). | Add GitHub Actions cache for `dbt_project/dbt_packages/`: `actions/cache` with key based on `packages.yml` hash. Also cache `dbt_project/target/` for `--defer` in incremental builds. | Low | 30min |
| **IS-06** | **No DAG-level SLA** — the `DbtDag` has no `sla` parameter. If the pipeline takes longer than expected (e.g., Snowflake warehouse resume, data volumes spike), there's no notification. | Add `sla=timedelta(hours=4)` to the DAG's `default_args`. Configure `sla_miss_callback` in Airflow to route SLA misses to Slack/email. | Low | 30min |

---

## Summary of Remaining Work by Priority

### Critical (0 items)
All critical issues from prior reviews have been addressed.

### High (5 items)
| ID | Area | Item | Effort |
|----|------|------|--------|
| S-01 | Security | Hash-pin Python dependencies | 1h |
| S-02 | Security | Pin Docker image to SHA digest | 10min |
| SC-03 | Scalability | Document Snowflake partition/warehouse strategy | 1h |
| DP-01 | Developer Practices | Add devcontainer configuration | 2h |
| IS-01 | Integration Stability | Add Snowflake CI job | 3–4h |

### Medium (11 items)
| ID | Area | Item | Effort |
|----|------|------|--------|
| A-01 | Architecture | Create `int_orders` intermediate model | 2–3h |
| A-03 | Architecture | Create `exposures.yml` for BI tool lineage | 30min |
| P-01 | Performance | Dedup post-hook for fct_orders incremental | 1–2h |
| P-02 | Performance | Automate Snowflake clustering in CD | 2h |
| S-04 | Security | Add SBOM generation to CI | 1h |
| S-05 | Security | Use Airflow Connections for Snowflake credentials | 2h |
| S-06 | Security | Add `pip-audit` vulnerability scanning to CI | 30min |
| S-07 | Security | Add Dependabot configuration | 30min |
| SC-01 | Scalability | Define Snowflake warehouse sizing strategy | 30min |
| SC-05 | Scalability | Add Cosmos `RenderConfig` for faster DAG parsing | 30min |
| DP-02 | Developer Practices | Add `make setup` target | 15min |

### Low (14 items)
| ID | Area | Item | Effort |
|----|------|------|--------|
| A-02 | Architecture | Add holiday/fiscal calendar to dim_dates | 1–2h |
| A-05 | Architecture | Tune source freshness thresholds | 5min |
| P-03 | Performance | Document incremental scaling triggers | 1h |
| P-04 | Performance | Document DuckDB pool/parallelism strategy | 30min |
| P-05 | Performance | Add query performance benchmarking | 1h |
| SC-02 | Scalability | Add snapshot growth monitoring test | 1h |
| SC-04 | Scalability | Document production `_ingested_at` pattern | 1h |
| CQ-03 | Code Quality | Remove default empty string from Snowflake account | 5min |
| CQ-04 | Code Quality | Add type annotations to ci_setup.py | 10min |
| CQ-05 | Code Quality | Use `adapter.quote()` in custom test macros | 10min |
| CQ-06 | Code Quality | Fix float comparison in row count test | 5min |
| DP-04 | Developer Practices | Add `make setup` target | 5min |
| DP-06 | Developer Practices | Document or remove `profiles.yml.template` | 5min |
| IS-05 | Integration Stability | Add GitHub Actions caching for dbt packages | 30min |

---

## Specific File References for Each Remaining Recommendation

### A-01 — Create `int_orders.sql`

**Location:** `dbt_project/models/intermediate/int_orders.sql` (new file)

```sql
{{
    config(
        materialized='table'
    )
}}

SELECT
    invoice_no,
    customer_id,
    MIN(invoice_date) AS order_date,
    COUNT(*) AS line_item_count,
    SUM(quantity) AS total_quantity,
    SUM(CASE WHEN is_return THEN 0 ELSE 1 END) AS forward_item_count,
    SUM(quantity * unit_price) AS total_order_value,
    SUM(CASE WHEN is_return THEN 0 ELSE quantity * unit_price END) AS forward_order_value,
    BOOL_OR(is_return) AS has_returns,
    COUNT(DISTINCT stock_code) AS unique_items
FROM {{ ref('stg_online_retail') }}
GROUP BY invoice_no, customer_id
```

**Add to `schema.yml`:** Columns with `not_null`, `unique` on `invoice_no`, `positive_value` on `total_order_value`, `accepted_values` on `has_returns`.

---

### A-03 — Create `exposures.yml`

**Location:** `dbt_project/models/exposures.yml` (new file)

```yaml
version: 2

exposures:
  - name: customer_360_dashboard
    type: dashboard
    maturity: low
    url: https://github.com/ak/cloud-native-analytics-pipeline
    description: >
      Customer-level dashboard showing segments, order history, and returns
    depends_on:
      - ref('dim_customers')
      - ref('fct_orders')

  - name: product_performance_report
    type: dashboard
    maturity: low
    url: https://github.com/ak/cloud-native-analytics-pipeline
    description: >
      Product-level sales trends, price analysis, and return rates
    depends_on:
      - ref('dim_products')
      - ref('fct_orders')
```

---

### P-01 — fct_orders incremental dedup post-hook

**File:** `dbt_project/models/marts/fct_orders.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='sync_all_columns',
        cluster_by='date_key',
        post_hook=[
            "{% if is_incremental() and target.type == 'duckdb' %}
                DELETE FROM {{ this }}
                WHERE (order_item_id, invoice_date) NOT IN (
                    SELECT order_item_id, MAX(invoice_date)
                    FROM {{ this }}
                    GROUP BY order_item_id
                );
            {% endif %}"
        ]
    )
}}
```

---

### S-01 — Hash-pinned requirements

Generate hash-pinned files:

```bash
# Install pip-tools
pip install pip-tools

# Create requirements.in (without hashes) for development
# Create requirements.txt (with hashes) for CI/CD/Docker
pip-compile --generate-hashes requirements.in > requirements.txt

# Same for airflow/

# Add a "pip-audit" step to CI that fails on known vulnerabilities:
pip install pip-audit
pip-audit -r airflow/requirements.txt -r requirements-dev.txt
```

---

### S-02 — Pin Docker image to SHA digest

**File:** `airflow/Dockerfile`

Change:
```dockerfile
FROM apache/airflow:2.10.4-python3.12
```

To:
```dockerfile
FROM apache/airflow@sha256:<computed_sha256_of_2.10.4-python3.12>
```

Compute with:
```bash
docker pull apache/airflow:2.10.4-python3.12
docker inspect --format='{{index .RepoDigests 0}}' apache/airflow:2.10.4-python3.12
```

---

### SC-03 — Snowflake warehouse strategy documentation

**File:** `dbt_project/profiles.yml`

```yaml
snowflake:
  outputs:
    prod:
      # Use a medium warehouse for production builds; ensure auto-resume is ON
      warehouse: DBT_WH
      # Production warehouse: ELT_WH (medium, multi-cluster, auto-suspend 5 min)
      # BI warehouse: BI_WH (small, multi-cluster, auto-suspend 1 min) — use for BI tools only
      threads: 8
      retry_on_database_errors: true
      client_session_keep_alive: true
```

---

### IS-01 — Snowflake CI job

**File:** `.github/workflows/ci.yml`

Add a `snowflake-build` job (if Snowflake credentials are available as org secrets):

```yaml
snowflake-build:
  if: github.repository_owner == 'your-org'
  needs: [lint-and-test]
  runs-on: ubuntu-latest
  steps:
    - name: Checkout
      uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af68
    - name: Setup Python
      uses: actions/setup-python@0b93645e9fea7318ecaed2b359559ac225c90a2
      with:
        python-version: '3.11'
    - name: Install deps
      run: pip install dbt-duckdb dbt-snowflake
    - name: Run dbt build on Snowflake (select modified models only)
      if: github.event_name == 'pull_request'
      env:
        SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
        SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
        SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
        DBT_PROFILE: snowflake
        DBT_TARGET: prod
      run: |
        cd dbt_project
        dbt deps --profiles-dir .
        dbt build --profiles-dir . --select state:modified+ --defer --state ../target
```

---

## Files Requiring No Further Action

These files have been fully remediated across all prior reviews:

| File | Status | Notes |
|------|--------|-------|
| `scripts/ingest_raw.py` | ✅ | Error handling, parameterized CSV, `_ingested_at` column |
| `scripts/ci_setup.py` | ✅ | Imports clean, env-based paths, float casting |
| `scripts/bootstrap_env.sh` | ✅ | POSIX-safe path resolution |
| `scripts/backup.sh` | ✅ | Proper tar+gzip, `set -euo pipefail` |
| `airflow/.env` | ✅ | Added to .gitignore |
| `airflow/.env.example` | ✅ | Full credential documentation |
| `airflow/docker-compose.yaml` | ✅ | Localhost-only ports, init container pool creation |
| `airflow/dags/dbt_cosmos_dag.py` | ✅ | No module-level DB queries, no except:pass, proper datetime handling |
| `airflow/Dockerfile` | ⚠️ | Only SHA pinning remains |
| `dbt_project/profiles.yml` | ⚠️ | Only Snowflake account default remains |
| `dbt_project/models/marts/fct_orders.sql` | ✅ | SCD2 filter, country column, 3-day lookback, cluster_by comment |
| `dbt_project/models/marts/dim_dates.sql` | ✅ | ISO week number, dbt.date_spine |
| `dbt_project/models/schema.yml` | ✅ | Correct FK references, expression_is_true deprecation fixed |
| `dbt_project/models/sources.yml` | ✅ | _ingested_at freshness |
| `dbt_project/models/staging/stg_online_retail.sql` | ✅ | Explicit column list, meaningful ORDER BY, dedup applied |
| `dbt_project/models/intermediate/int_customer_metrics.sql` | ✅ | Single-scan pattern |
| `dbt_project/snapshots/dim_customers_snapshot.sql` | ✅ | check_cols trimmed |
| `dbt_project/snapshots/dim_products_snapshot.sql` | ✅ | check_cols uses min/max_price |
| `.gitignore` | ✅ | Comprehensive exclusions |
| `.pre-commit-config.yaml` | ✅ | Full hook suite |
| `.github/workflows/ci.yml` | ✅ | SHA-pinned, secret scanning, freshness check, row count verify |
| `.github/workflows/docs.yml` | ✅ | dbt build before docs, SHA-pinned |
| `pyproject.toml` | ✅ | ruff/pytest/mypy |
| `Makefile` | ✅ | build-models, freshness targets |
| `CONTRIBUTING.md` | ✅ | Full dev setup, testing instructions |
| `.github/PULL_REQUEST_TEMPLATE.md` | ✅ | Checklist with build/lint |
| `SECURITY.md` | ✅ | Disclosure policy, credential handling |

---

## Conclusion

The project has advanced from its initial state through significant remediation across all 7 dimensions. The remaining work is largely **Phase 2 maturity** — moving from "correct and secure" to "production-hardened and scalable."

**Immediate priorities (High, 1–2 weeks):**
1. Hash-pin Python dependencies and pin Docker to SHA digest (S-01, S-02)
2. Add Snowflake CI job to catch dialect/connection issues before CD (IS-01)
3. Add devcontainer configuration for zero-friction onboarding (DP-01)
4. Document Snowflake warehouse + partitioning strategy (SC-03)

**Next sprint (Medium, 2–4 weeks):**
5. Create `int_orders` intermediate model for order-level analytics (A-01)
6. Create `exposures.yml` for BI tool lineage (A-03)
7. Add `pip-audit` + Dependabot for vulnerability management (S-06, S-07)
8. Add Cosmos `RenderConfig` for faster DAG parsing at scale (SC-05)
9. Automate Snowflake clustering in CD pipeline (P-02)
10. Add `make setup` target and devcontainer (DP-01, DP-04)

**Backlog (Low, as needed):**
- Holiday/fiscal calendar in dim_dates, SFTP monitoring DAG, query benchmarking, `adapter.quote()` in custom tests, SBOM in CI, Airflow SLA miss notification.

---

*Research conducted: 2026-06-28 | Based on comprehensive analysis of 50+ source files, 3 prior review documents (ANALYSIS_REPORT.md, IMPROVEMENT_PLAN.md, REVIEW.md), and 137+ passing dbt tests.*
