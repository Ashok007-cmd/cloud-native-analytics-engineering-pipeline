# Improvement Plan — Cloud-Native Analytics Engineering Pipeline

**This file replaces all prior versions of itself.** Everything below was verified by actually
running the tools on 2026-07-05, not by reading previous reports (several of which contained
fabricated claims of git/GitHub activity that never happened — see the "Fixed today" table, #1 and
the note at the end of this file). Where an earlier plan is contradicted here, trust this one.

## Verified baseline (before today's fixes)

| Check | Result |
|---|---|
| `dbt build` (DuckDB dev target, 1,067,371 ingested rows) | PASS=159 WARN=1 ERROR=0 NO-OP=3 TOTAL=163 |
| `pytest tests/` | 34 passed, 96% coverage (`scripts/`, `airflow/dags/`) |
| `ruff check scripts/ airflow/dags/` | clean |
| `sqlfluff lint` (models + snapshots) | clean |
| `pip-audit` — `requirements-dev.txt`, `dbt_project/requirements-snowflake.txt` | clean |
| `pip-audit` — `airflow/requirements.txt` | **33 known CVEs** (apache-airflow 2.10.5 + transitive flask/werkzeug/providers) |
| `make security` | **broken** — shelled out to `git ls-files`; this project has no `.git` |
| `README.md` | contained fabricated CI badges, a fake GitHub Pages link, and false claims of pushed/running GitHub Actions |

## Fixed today (verified after each change)

| # | Area | File | Issue | Fix |
|---|------|------|-------|-----|
| 1 | Integrity | `README.md` | Fabricated CI badges linking to a repo that was never pushed, a fake live GitHub Pages link, and prose claiming GitHub Actions runs that never happened (no `.git` exists anywhere in this project) | Rewrote to describe only verified, actually-true state; added a status note pointing at the retraction |
| 2 | Security | `airflow/requirements.in` / `requirements.txt` | `apache-airflow==2.10.5`, capped at `<2.11.0` — excludes the patched `2.11.1`, leaving 33 known CVEs unaddressed | Widened ceiling to `<2.12.0`, recompiled with `uv pip compile --generate-hashes`. Now `apache-airflow==2.11.2`, `astronomer-cosmos==1.5.1` (resolved cleanly). CVE count: 33 → 20 (see "Remaining" below) |
| 3 | DevOps | `Makefile:93` (`security` target) | Called `git ls-files` — fails outright since this project has never been a git repo, breaking `make security` and `make validate-all` | Replaced with a `find`-based file list excluding vendored/generated paths (`dbt_packages`, `target`, logs, `.duckdb`, `.csv`, etc.) |
| 4 | Data correctness | `dbt_project/snapshots/dim_products_snapshot.sql:10` | `check_cols` omitted `avg_price`, `order_count`, `total_revenue` — a price/volume shift without a min/max change would silently skip an SCD2 row, leaving `dim_products` stale | Added the omitted columns to `check_cols` |
| 5 | Data correctness | `dbt_project/snapshots/dim_customers_snapshot.sql:10` | Same gap: `check_cols` omitted `gross_revenue`, `return_count`, `total_items`, `total_quantity`, `recency_days` | Added the omitted columns to `check_cols` |
| 6 | Code quality | `dbt_project/models/schema.yml:297` | The `dim_dates → fct_orders` relationships test checks the *wrong direction* (every calendar day must have an order) — permanently warns (148 results) on any real retail dataset since weekends/holidays have no sales; the correct FK check (`fct_orders.date_key → dim_dates`) already exists and passes separately | Documented as an intentional, non-actionable coverage signal so it stops reading as an unresolved bug |
| 7 | Security | `scripts/bootstrap_env.sh` | Global `sed` replace on the literal placeholder `<change_me>` — since `.env.example` uses that same placeholder for **both** `AIRFLOW_PG_PASSWORD` and `AIRFLOW_ADMIN_PASSWORD`, a fresh run assigned the database password and the UI admin password the *same* value | Generate two independent random values; replace per full `KEY=<change_me>` line, not by placeholder text alone |
| 8 | Security | `airflow/.env`, `scripts/bootstrap_env.sh` | Generated `.env` was world-readable (`664`) — any local user could read live Fernet key / DB password / admin password | `chmod 600` the existing file; script now does this on every future run |
| 9 | Security | `airflow/docker-compose.yaml` (`airflow-init`) | `--password "${AIRFLOW_ADMIN_PASSWORD:-admin}"` silently falls back to the well-known default `admin/admin` if the env var is unset | Fails hard with a clear error instead of defaulting |
| 10 | Correctness / Data safety | `scripts/backup.sh` | `make backup` deleted the **original** `dev.duckdb` after archiving it — surprising and destructive for an operation named "backup," with no restore script or Makefile target to recover it | Removed the deletion; backup now only archives. Added `scripts/restore.sh` + `make restore FILE=...` target, plus round-trip tests |
| 11 | Consistency | `airflow/docker-compose.yaml:5` | Image tag `local-airflow-cosmos:2.10.4` no longer matched the pinned Python package version | Bumped to `2.11.2` |
| 12 | Test coverage | `tests/test_scripts.py` | New `restore.sh` had no test coverage; the old backup test asserted the destructive (now-removed) delete behavior | Updated the backup assertion, added 2 new tests (round-trip restore, missing-file rejection) |

**Verified after fixes:** `dbt build` → PASS=159 WARN=1 ERROR=0 TOTAL=163 (unchanged, confirms no regressions). `pytest` → 36 passed, 96% coverage. `ruff` / `sqlfluff` → clean.

## Remaining CVEs in `airflow/requirements.txt` (20, down from 33)

Bumping to Airflow 2.11.2 fixed 4 CVEs outright (PYSEC-2026-10, CVE-2025-65995, CVE-2024-56373,
CVE-2025-27555) and reduced several others. What's left all requires a major-version jump to
**Airflow 3.x**, which has breaking DAG-authoring API changes — not something to silently force
through a dependency bump. (Corrected 2026-07-05: an earlier pass of this table under-counted the
three provider rows below at 17 total — re-verified with a fresh `pip-audit` run; 20 is the
accurate current count.)

| Package | Version | Needs |
|---|---|---|
| `apache-airflow` | 2.11.2 | 3.1.1 – 3.2.2 (9 separate CVEs/PYSECs) |
| `apache-airflow-providers-fab` | 1.5.4 | 3.6.4 |
| `apache-airflow-providers-ftp` | 3.13.3 | 3.15.1 |
| `apache-airflow-providers-http` | 5.5.0 | 6.0.0 |
| `apache-airflow-providers-smtp` | 2.3.2 | 3.0.0 |
| `flask` | 2.3.3 | 3.1.3 |
| `flask-appbuilder` | 4.5.4 | 4.6.2 / 4.8.1 |

**Recommendation:** treat the Airflow 3.x migration as its own tracked phase — read the
[Airflow 3 upgrade guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading.html),
update `airflow/dags/dbt_cosmos_dag.py` and `astronomer-cosmos` together, and re-run the full
pytest + dbt build suite before promoting. Don't schedule it casually — budget a session for it.

## Resolved: stale/fabricated report files

`AUDIT_REPORT.md` and `CLEANUP_REPORT.md` described git pushes, GitHub Actions runs, and a live
GitHub Pages deploy that never happened — **deleted** (2026-07-05, by explicit decision; no git
history existed to make this reversible, so this was confirmed before acting). The false addendum
in `FINAL_SECURITY_AND_IMPROVEMENT_REPORT.md` §6.1 making the same claims one level up has been
retracted in place rather than deleted, since the rest of that file's findings (Sections 0–5, 7)
checked out as genuine. `ANALYSIS_REPORT.md`, `RESEARCH_REPORT.md`, and `REVIEW.md` still reference
`.github/workflows/*.yml` files that don't exist, so treat any CI-related claim in them as
unverifiable — their non-CI technical findings (SCD2 join logic, incremental dedup) looked genuine
on spot-check.

## Resolved: no CI/CD existed

Added `.github/workflows/ci.yml` (2026-07-05): three jobs — `lint` (ruff + sqlfluff),
`build-and-test` (dbt build against `scripts/ci_setup.py`'s synthetic 3k-row sample, then pytest),
and `security` (pip-audit + detect-secrets). GitHub Actions are pinned to commit SHAs (fetched live
via `gh api repos/actions/checkout/git/ref/tags/...`, not assumed). Each job's logic was dry-run
locally first — including a from-scratch `dbt deps` + `sqlfluff` pass with no pre-existing
`dbt_packages/`, and a full `dbt build` against the synthetic sample (159 pass / 1 warn / 0 error,
matching the real-data build) — before ever being pushed. **The README will only get a CI badge
once a real run has gone green on GitHub, verified via `gh run watch`** — not before, per the
lesson in the git-history-fabrication note above.

## Architecture — verified sound, no changes needed

`fct_orders.sql`'s incremental strategy (point-in-time SCD2 joins, 3-day late-arrival lookback,
post-hook dedup, `on_schema_change='fail'`) is correct and well-documented, including an explicit,
acknowledged limitation around late-arriving dimension changes. `dayofweek_expression()` correctly
dispatches per-adapter (DuckDB `isodow` vs Snowflake `dayofweekiso`) — both targets produce
consistent results, no portability bug. Materialization strategy (view/staging, table/intermediate,
table/marts, incremental override on `fct_orders`) is appropriate for data volume. No SQL-injection
risk in any macro (identifiers come from `schema.yml` config, not user input).

## Scalability — notes carried from `profiles.yml`, still valid

Snowflake clustering guidance already documented in `dbt_project/profiles.yml` (cluster
`fct_orders` on `(date_key, customer_key)`, revisit `dim_customers`/`dim_products` clustering only
past ~5M rows) matches the actual `snowflake_bootstrap.sql` setup (least-privilege roles, weekly
reclustering task as a backstop to automatic clustering, suspended by default). No changes needed
at current (~1M row) scale.
