---
phase: code-review
reviewed: 2026-07-03T15:00:00Z
depth: deep
files_reviewed: 35
files_reviewed_list:
  - scripts/ingest_raw.py
  - scripts/ci_setup.py
  - scripts/bootstrap_env.sh
  - scripts/backup.sh
  - tests/test_scripts.py
  - airflow/dags/dbt_cosmos_dag.py
  - airflow/Dockerfile
  - airflow/docker-compose.yaml
  - airflow/requirements.txt
  - dbt_project/dbt_project.yml
  - dbt_project/profiles.yml
  - dbt_project/packages.yml
  - dbt_project/package-lock.yml
  - dbt_project/models/staging/stg_online_retail.sql
  - dbt_project/models/intermediate/int_customer_metrics.sql
  - dbt_project/models/intermediate/int_product_metrics.sql
  - dbt_project/models/marts/dim_dates.sql
  - dbt_project/models/marts/dim_customers.sql
  - dbt_project/models/marts/dim_products.sql
  - dbt_project/models/marts/fct_orders.sql
  - dbt_project/models/schema.yml
  - dbt_project/models/sources.yml
  - dbt_project/models/exposures.yml
  - dbt_project/macros/dayofweek_expression.sql
  - dbt_project/macros/month_name_expression.sql
  - dbt_project/macros/revenue_calculations.sql
  - dbt_project/macros/test_not_in_future.sql
  - dbt_project/macros/test_positive_value.sql
  - dbt_project/snapshots/dim_customers_snapshot.sql
  - dbt_project/snapshots/dim_products_snapshot.sql
  - dbt_project/tests/assert_no_high_value_customers_with_negative_revenue.sql
  - dbt_project/tests/assert_fct_orders_row_count_matches_staging.sql
  - pyproject.toml
  - requirements-dev.in
  - requirements-dev.txt
  - Makefile
  - .sqlfluff
  - .pre-commit-config.yaml
  - .secrets.baseline
  - .gitignore
  - .github/dependabot.yml
  - .github/workflows/ci.yml
  - .github/workflows/cd.yml
  - .github/workflows/docs.yml
findings:
  critical: 1
  warning: 5
  info: 4
  total: 10
status: issues_found
---

# Phase: Code Review Report — Follow-up Review (post-28-fix)

**Reviewed:** 2026-07-03T15:00:00Z
**Depth:** deep (cross-module analysis, empirical verification via live `dbt build`/`pip install` runs)
**Files Reviewed:** 35 source/config files
**Status:** issues_found

## Summary

This is an independent, fresh-eyes review of the codebase **after** all 9 CRITICAL, 11 WARNING, and 8 INFO findings from the prior review were fixed via the `fix(CR-XX)/fix(WR-XX)/fix(IN-XX)` commit series. I read the current state of every file directly (not just the diff) and empirically verified behavior where the code's own comments made testable claims — including running `dbt build`/`dbt run` against a live DuckDB and running `pip install` against the pinned requirement files in isolated virtualenvs.

**All 9 original CRITICAL findings are genuinely fixed**, not just superficially patched — see the verification section below, including two (CR-02, CR-04) where I traced the new logic for correctness rather than trusting the commit message.

However, this review found **1 new CRITICAL issue** that the original review missed entirely: **the hash-pinned `requirements-dev.txt` and `airflow/requirements.txt` contain fabricated/incorrect SHA-256 hashes and are missing transitive dependencies.** I verified this empirically — `pip install -r requirements-dev.txt` and `pip install -r airflow/requirements.txt` both fail on a clean virtualenv. Because pip auto-enables hash-checking mode whenever *any* hash is present in a requirements file, this isn't a `--require-hashes`-only problem: it breaks the plain `pip install -r requirements-dev.txt` step used in **`ci.yml`, `cd.yml`, and `docs.yml`**, as well as the Airflow Docker image build and `make setup`. In its current committed state, this repository's CI/CD would fail on the very first dependency-install step of every workflow. This is more severe than any of the original 9 findings because it doesn't just corrupt data or misroute a build — it prevents the pipeline from running at all.

The remaining findings are WARNING/INFO-level robustness and documentation-accuracy issues, including one interesting case where the CR-02 fix's inline comment makes a factual claim about DuckDB's default incremental behavior that I disproved by inspecting the installed `dbt-duckdb` 1.10.1 adapter source and compiling the actual SQL dbt generates (see WR-01 below) — the post-hook dedup logic is currently dead weight, not a live bug, but the comment misdiagnoses why it's there.

---

## Verification of the Original 9 CRITICAL Findings

| ID | Original issue | Status | Notes |
|----|-----------------|--------|-------|
| CR-01 | `tar` backup without checkpoint → corrupt archive | **Fixed** | `scripts/backup.sh:31-45` now runs `CHECKPOINT` via a Python/duckdb subprocess before `tar`, and hard-fails (`exit 1`) if the checkpoint fails. Verified: original file is only removed after a successful, non-empty archive (`backup.sh:50-64`). |
| CR-02 | Post-hook dedup: duplicate window + equal-date stalemate | **Fixed, but built on a false premise** | The equal-date stalemate is genuinely fixed (`rowid DESC` tiebreak, `fct_orders.sql:29-32`). However, I verified empirically (see WR-01) that the "duplicate window" this post-hook exists to close does not occur with the project's actual default incremental strategy (`delete+insert`, not the appending "MERGE" the comment describes). The fix is correct and safe, but is solving a problem that doesn't exist under the current adapter defaults — see WR-01 for the documentation-accuracy issue this creates. |
| CR-03 | Staging dedup `ORDER BY description, country` arbitrary | **Fixed** | `stg_online_retail.sql:48-51` now orders by `LENGTH(description) DESC, description ASC, country ASC` — deterministic and favors the more complete description. |
| CR-04 | Dimension joins used `dbt_valid_to IS NULL` (current-only) | **Fixed, with a residual staleness caveat** | `fct_orders.sql:71-95, 117-126` now does genuine point-in-time joins with the earliest version floored to `1900-01-01`. I traced the single-version, tie, and boundary edge cases (see below) — all correct. However, see WR-04 for a real (not hypothetical) SCD-fact staleness gap that the point-in-time redesign didn't address: already-materialized fact rows are never re-evaluated when a dimension version that was "current" (open-ended) at insert time later closes. |
| CR-05 | `on_schema_change='sync_all_columns'` auto-drops columns | **Fixed** | `fct_orders.sql:9` — now `'fail'`. |
| CR-06 | Dead `int_orders` model | **Fixed** | Confirmed absent from `dbt_project/models/intermediate/` (only `int_customer_metrics.sql` and `int_product_metrics.sql` remain); no dangling refs in `schema.yml` or `exposures.yml`. |
| CR-07 | Snowflake CI job fails on first run (no manifest) | **Fixed** | `ci.yml:129-146` adds a "Generate baseline manifest if none exists" step running `dbt parse` before the `--defer --state` build. |
| CR-08 | CD pipeline missing `--profile snowflake` | **Fixed** | `cd.yml:43,54` — both `dbt debug` and `dbt build` now pass `--profile snowflake --target prod`. |
| CR-09 | `.secrets.baseline` excludes `.env` from scanning | **Fixed** | `.secrets.baseline`'s `should_exclude_file` pattern now only excludes `.+\.duckdb$` / `.+\.duckdb\.wal$` — the `.env` exclusion is gone. `ci.yml:37-43` also now hard-fails on `detect-secrets-hook` output instead of only warning. |

**Point-in-time SCD2 edge cases traced for CR-04** (as specifically requested):
- **Single-version customer/product** (never changed): `MIN(dbt_valid_from) OVER (PARTITION BY customer_id)` equals its own `dbt_valid_from`, so it's floored to `1900-01-01`; `dbt_valid_to` is `NULL` → `9999-12-31`. Range covers all time. Verified empirically against a live single-run build (0 rows fell back to the `'-1'` sentinel).
- **Tied `dbt_valid_from` within a partition**: not possible in practice — dbt's `check` strategy snapshot assigns exactly one `dbt_valid_from`/version per unique key per invocation, so a customer can have at most one "new" row per snapshot run; no intra-partition ties.
- **Range contiguity**: relies on dbt's snapshot mechanics setting the closed row's `dbt_valid_to` to exactly the new row's `dbt_valid_from` — standard `check`-strategy behavior, not something this codebase controls, but correctly assumed.

---

## New Critical Issues

### CR-01: `requirements-dev.txt` and `airflow/requirements.txt` have fabricated hashes and missing transitive dependencies — breaks every CI/CD install step and the Airflow Docker build

**File:** `requirements-dev.txt:8-19`, `airflow/requirements.txt:8-19`
**Severity:** CRITICAL — pipeline cannot install dependencies in a clean environment

**Issue:** Both files were introduced in commit `ea4caf3` ("hash-pin all requirement files ... via PyPI JSON API") but the hashes they contain do not match the real packages on PyPI, and neither file lists any transitive dependencies.

I verified this empirically in two independent clean virtualenvs:

```
$ python3 -m venv /tmp/v1 && /tmp/v1/bin/pip install --require-hashes -r requirements-dev.txt
...
ERROR: In --require-hashes mode, all requirements must have their versions pinned with ==. These do not:
    appdirs from https://files.pythonhosted.org/packages/.../appdirs-1.4.4-py2.py3-none-any.whl
    (from sqlfluff==3.0.0->-r requirements-dev.txt (line 8))
```

```
$ python3 -m venv /tmp/v2 && /tmp/v2/bin/pip install --no-cache-dir -r airflow/requirements.txt
...
ERROR: THESE PACKAGES DO NOT MATCH THE HASHES FROM THE REQUIREMENTS FILE. ...
    duckdb==1.5.0 from https://files.pythonhosted.org/packages/.../duckdb-1.5.0-....whl:
        Expected sha256 4a2cd73d50ea2c2bf618a4b7d22fe7c4115a1c9083d35654a0d5d421620ed999
             Got        6e56c19ffd1ffe3642fa89639e71e2e00ab0cf107b62fe16e88030acaebcbde6
```

I confirmed the real published hash for `sqlfluff==3.0.0`'s wheel via `pip download` + `pip hash`: `362a70cbf3f8d72b0e8687c0c04e61286ed424fd6cc9d4a79b079cba9be62d4d` — completely different from the `bd91d52b...` value committed in `requirements-dev.txt:9`. These are not stale hashes from an older release; they don't correspond to any real artifact for that package/version.

**Why this is worse than a normal broken lockfile:** pip auto-enables hash-checking mode the moment *any* requirement in a file has a hash — you don't need `--require-hashes` to trigger it. That means:
- `.github/workflows/ci.yml:23` (`pip install -r requirements-dev.txt`) — fails
- `.github/workflows/cd.yml:29` (`pip install -r requirements-dev.txt`) — fails
- `.github/workflows/docs.yml:29` (`pip install -r requirements-dev.txt`) — fails
- `Makefile:44` (`pip install --require-hashes -r requirements-dev.txt`, the `setup` target) — fails
- `airflow/Dockerfile:14` (`pip install --no-cache-dir -r requirements.txt`) — fails, so the production Airflow image cannot be built at all

Every workflow entry point and the deployable artifact itself are broken by this file in its current committed state.

**Risk of NOT fixing:** CI is red on every push/PR from a clean runner; the CD deploy job cannot even install `dbt-snowflake`; the Airflow image referenced by `docker-compose.yaml` cannot be rebuilt. This would be immediately visible on the next real CI run, but it slipped past all 28 prior fixes because none of them touched dependency installation, and local development environments (like the one used to validate "make lint / build-models / test-py pass") already have packages installed from before this file broke, so the breakage is invisible locally.

**Fix:** Regenerate both files for real using the documented process, and verify the result installs cleanly before committing:

```bash
pip install pip-tools
pip-compile --generate-hashes requirements-dev.in > requirements-dev.txt
pip-compile --generate-hashes airflow/requirements.in > airflow/requirements.txt   # if requirements.in exists; otherwise author one first

# Then verify in a clean venv before committing:
python3 -m venv /tmp/verify && /tmp/verify/bin/pip install --require-hashes -r requirements-dev.txt
```

Add a CI step (or pre-commit hook) that actually performs a clean-venv install of these files, so a future hand-edited or hallucinated hash can never reach `main` undetected:

```yaml
- name: Verify hash-pinned requirements install cleanly
  run: |
    python3 -m venv /tmp/verify
    /tmp/verify/bin/pip install --require-hashes -r requirements-dev.txt
```

---

## Warnings

### WR-01: `fct_orders` post-hook dedup comment misdiagnoses DuckDB's actual incremental behavior — the workaround is currently dead weight

**File:** `dbt_project/models/marts/fct_orders.sql:12-37`
**Severity:** WARNING — misleading documentation + unnecessary full-table work every incremental run

**Issue:** The post-hook's comment (and the CR-02 fix commit message) states: *"DuckDB's MERGE with unique_key does not deduplicate on conflict; it appends."* I verified this is not true for the currently pinned adapter. No `incremental_strategy` is configured on `fct_orders`, so dbt resolves to `'default'`, and for `dbt-duckdb` (installed version `1.10.1`, matching the project's pin) the default strategy macro is:

```
# .venv/.../dbt/include/duckdb/macros/adapters.sql
{% macro duckdb__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}
```

i.e. **`delete+insert`**, not `merge`. I confirmed this by running `dbt run --select fct_orders` twice against a live DuckDB and inspecting the actual compiled/executed SQL in `target/run/.../fct_orders.sql`:

```sql
delete from "..."."analytics_marts"."fct_orders"
where (order_item_id) in (select (order_item_id) from "fct_orders__dbt_tmp...");

insert into "..."."analytics_marts"."fct_orders" (...)
(select ... from "fct_orders__dbt_tmp...")
```

This `delete+insert` runs inside the single transaction dbt already opens for the incremental materialization (`BEGIN` before, `COMMIT` after) — so there is no visible duplicate window for concurrent readers, and no appending merge behavior to work around. The post-hook is therefore currently a no-op every run (it re-ranks and re-deletes from an already-duplicate-free table), at the cost of a full-table window-function scan on every incremental build.

This isn't a correctness bug (the post-hook is idempotent and harmless), but it means: (1) the code comment actively misinforms future maintainers about why the post-hook exists, and (2) if someone changes the adapter, an override, or the model's `incremental_strategy` config in the future, they'll be relying on documentation that was already wrong before their change.

**Fix:** Either remove the post-hook (since the default `delete+insert` strategy already handles dedup atomically), or — if it's being kept deliberately as defense-in-depth against a future strategy change — correct the comment to say so explicitly instead of asserting a behavior that isn't what's happening today:

```sql
-- Defense-in-depth only: dbt-duckdb's DEFAULT incremental strategy for this
-- model is delete+insert (verified: `dbt-duckdb` resolves unset
-- incremental_strategy to get_incremental_delete_insert_sql), which already
-- performs an atomic, transactional dedup on `unique_key`. This post-hook is
-- a no-op today. It exists only to guard against a future `incremental_strategy`
-- override (e.g. `append` or `merge`) reintroducing duplicates silently.
```

---

### WR-02: `make security` does not actually gate on the secrets baseline — always reports success

**File:** `Makefile:89-93`
**Severity:** WARNING — false sense of security for local `make validate-all` runs

**Issue:**

```makefile
security:
	@echo "--- Running pip-audit ---"
	pip-audit -r requirements-dev.txt || echo "::warning::Vulnerabilities found — review pip-audit output above"
	@echo "--- Running detect-secrets ---"
	detect-secrets scan > /dev/null && echo "No unsealed secrets found" || echo "::warning::Secrets scan baseline updated"
```

`detect-secrets scan` (without `--baseline .secrets.baseline`) always exits `0` — it doesn't compare against the existing baseline at all, it just regenerates a fresh baseline to stdout, which is discarded here (`> /dev/null`). This is the exact anti-pattern `ci.yml`'s own comment calls out and correctly avoids (`ci.yml:37-41`: *"detect-secrets scan ... always exits 0 and only regenerates a baseline ... detect-secrets-hook exits non-zero when it finds secrets not already audited in the baseline"*). The CI workflow does it right (`detect-secrets-hook --baseline .secrets.baseline $(git ls-files)`); the Makefile target that a developer would actually run locally via `make validate-all` does it wrong, and will print "No unsealed secrets found" even when a real secret has been introduced.

**Fix:** Use the same hard-fail mechanism as CI:

```makefile
security:
	@echo "--- Running pip-audit ---"
	pip-audit -r requirements-dev.txt || echo "::warning::Vulnerabilities found — review pip-audit output above"
	@echo "--- Running detect-secrets ---"
	detect-secrets-hook --baseline .secrets.baseline $(shell git ls-files) && echo "No new secrets found"
```

---

### WR-03: `fct_orders.customer_key`/`product_key` relationship tests have no `severity: warn` fallback, but the model explicitly builds an unsupported `'-1'` sentinel key

**File:** `dbt_project/models/schema.yml:314-329`, `dbt_project/models/marts/fct_orders.sql:106-107`
**Severity:** WARNING — a legitimate "unknown dimension" case would hard-fail the entire `dbt build`, not just warn

**Issue:** `fct_orders.sql` explicitly codes a fallback: `COALESCE(customers.customer_key, '-1') AS customer_key` (and the same pattern for `product_key`), documented in `README_DBT.md:197` as intentional ("Unknown dimension keys coalesced"). But there is no seed/default row with `customer_key = '-1'` in `dim_customers_snapshot`, and the `relationships` tests on `fct_orders.customer_key`/`product_key` (`schema.yml:314-329`) have no `severity: warn` override — unlike the `date_key` relationship test just above them (`schema.yml:333-341`), which explicitly does.

In the current single-run demo dataset this never triggers (verified: 0 rows with `customer_key='-1'` or `product_key='-1'` in a live build) because the snapshot and the fact table are always built from the same staging snapshot within one `dbt build` invocation. But the moment there's any timing skew between when a new customer/product first appears in staging and when the SCD2 snapshot next captures it — e.g. someone runs `dbt run --select fct_orders` without first re-running the snapshots, or a partial/manual Airflow re-run — this code path is designed to produce `'-1'`, and the very next `dbt test` will hard-fail the whole build on a `not_null`-adjacent `relationships` test with no severity override, rather than degrading gracefully the way `date_key` does for the exact same class of "unknown dimension key" scenario.

**Fix:** Either add a `severity: warn` override to the `customer_key`/`product_key` relationship tests (consistent with how `date_key` is already handled), or seed an explicit `'-1'`/"Unknown" row into `dim_customers_snapshot` and `dim_products_snapshot` so the FK actually resolves and the test can stay strict:

```yaml
- name: customer_key
  tests:
    - not_null
    - relationships:
        arguments:
          to: ref('dim_customers_snapshot')
          field: customer_key
        config:
          severity: warn   # matches the '-1' unknown-dimension fallback pattern
```

---

### WR-04: Point-in-time SCD2 fact rows are never re-evaluated once written — a dimension version that closes after a fact row is inserted leaves that fact row silently stale

**File:** `dbt_project/models/marts/fct_orders.sql:71-95, 117-126, 128-139`
**Severity:** WARNING — real (not hypothetical) correctness gap in the CR-04 redesign, worth documenting even though it won't manifest for this specific one-shot historical dataset

**Issue:** The point-in-time join correctly bounds each dimension version by `[dbt_valid_from, dbt_valid_to)`. But once a fact row is written by an incremental run, it is **never revisited** — the `is_incremental()` predicate (`fct_orders.sql:128-139`) only ever looks at new/late-arriving rows from `stg_online_retail` by `invoice_date`, not at existing fact rows whose matched dimension version has since closed.

Concretely: if a fact row for `invoice_date = T` is inserted while a customer's *current* (open-ended, `dbt_valid_to IS NULL` → `9999-12-31`) dimension version is still open, it correctly joins to that version. If a later snapshot run subsequently closes that version at some `T_close <= T` (i.e., the customer changed again, and the change is detected retroactively relative to a fact row that's already landed with a timestamp at or after the new boundary), the already-materialized fact row keeps its original `customer_key` — dbt has no mechanism in this design to go back and re-join it to whichever version is now correct for `T`. This is the classic "late-arriving dimension" gap in type-2 fact tables. It's inherent to essentially any point-in-time fact design that doesn't also reprocess a dimension-driven lookback window (only a fact-driven one exists here via the 3-day `invoice_date` lookback), so it's not something the CR-04 fix could have fully closed without materially larger changes — but it's worth calling out explicitly since it wasn't mentioned in the original CR-04 write-up and someone relying on this table for historical segment-trend analysis should know it exists.

Given this pipeline's `check_cols` for `dim_customers_snapshot` include mutable aggregate metrics (`total_orders`, `total_revenue` — see `dim_customers_snapshot.sql:10`), a customer effectively gets a new SCD2 version on almost every snapshot run they have new activity in, which increases how often this gap can be hit relative to a dimension that only tracks slowly-changing attributes like address or tier.

**Fix:** Document this as a known limitation (minimum) or add a periodic re-materialization strategy that re-derives `customer_key`/`product_key` for a trailing window of already-loaded fact rows whenever the corresponding dimension is updated, not just when new invoices arrive:

```sql
-- KNOWN LIMITATION: once a fact row is written, its customer_key/product_key
-- reflect the dimension version that was open AT INSERT TIME. If a dimension
-- version that was open when a fact row was written later closes (e.g. a
-- customer's segment changes in a subsequent snapshot run, backdated relative
-- to an already-loaded invoice), the already-materialized fact row is not
-- retroactively re-joined. Mitigate by re-running `dbt build --full-refresh
-- --select fct_orders` after any dimension backfill, or by adding a
-- dimension-change-driven reprocessing window if this becomes a live concern.
```

---

### WR-05: `ingest_raw.py` final-count `assert` is not caught by the surrounding `except duckdb.Error` — would crash uncleanly instead of exiting via the script's own error-handling convention

**File:** `scripts/ingest_raw.py:111-120`
**Severity:** WARNING — inconsistent/unreachable-in-practice error handling, but a real gap if it ever did trigger

**Issue:**

```python
try:
    ...
    final_count = con.execute("SELECT COUNT(*) FROM raw.online_retail").fetchone()[0]
    logger.info("Ingested %d rows into raw.online_retail", final_count)
    assert final_count >= _MINIMUM_ROW_COUNT, (
        f"Final row count {final_count} below minimum {_MINIMUM_ROW_COUNT}"
    )
    sample = con.execute("SELECT * FROM raw.online_retail LIMIT 3").fetchall()
    logger.info("Sample:\n%s", sample)
except duckdb.Error as exc:
    with contextlib.suppress(duckdb.Error):
        con.execute("ROLLBACK")
    logger.error("Ingestion failed: %s", exc)
    sys.exit(1)
finally:
    con.close()
```

This `assert` (line 111-113) is effectively unreachable today because it duplicates a check already performed and enforced earlier (Step 5, lines 93-100) against the same table before the rename — the rename itself cannot change row count, so `final_count` will always equal the already-validated `row_count`. But if it ever *did* fail (e.g. a future refactor reorders these steps, or the rename semantics change), `AssertionError` is not a subclass of `duckdb.Error`, so it would **not** be caught by the surrounding `except duckdb.Error` clause. It would propagate past the `finally` (which correctly still closes the connection) and crash the script with a raw, unstructured Python traceback — bypassing the clean `logger.error(...)` + `sys.exit(1)` pattern this script otherwise uses consistently for every other failure mode. Also worth noting: `assert` statements are stripped entirely when Python runs with `-O`, silently removing this check.

**Fix:** Replace the `assert` with the same explicit check-and-exit pattern used at Step 5, and drop the now-fully-redundant per-file `ruff` ignore for `S101` on this file if this is the only assert removed:

```python
if final_count < _MINIMUM_ROW_COUNT:
    logger.error(
        "Post-swap row count %d below minimum %d — this should be unreachable; "
        "investigate the rename step", final_count, _MINIMUM_ROW_COUNT,
    )
    sys.exit(1)
```

---

## Info

### IN-01: Stray empty `requirements-dev.txt.new` file left in the working tree

**File:** `requirements-dev.txt.new`
**Severity:** INFO

**Issue:** An empty, untracked `requirements-dev.txt.new` (0 bytes) sits alongside `requirements-dev.txt`, apparently a leftover from an interrupted `pip-compile` attempt (plausibly while investigating CR-01 above). It doesn't affect any build but is confusing clutter for the next person to find it.

**Fix:** Delete it, or if it's meant to be the corrected output of CR-01's fix, actually populate it via `pip-compile` and rename it over `requirements-dev.txt`.

---

### IN-02: `ci_setup.py` module-level constant uses inconsistent casing

**File:** `scripts/ci_setup.py:15`
**Severity:** INFO

**Issue:** `_DEFAULT_dst = os.path.join(...)` mixes a leading-underscore/uppercase-prefix convention with a lowercase suffix (`dst`), inconsistent with the all-caps convention used by the adjacent `PROJECT_ROOT`, `SEED`, and `ROWS` constants on the same lines.

**Fix:** Rename to `_DEFAULT_DST` for consistency (2 references to update: definition and `os.environ.get("RAW_CSV_PATH", _DEFAULT_dst)` in `main()`).

---

### IN-03: `docs.yml` and `ci.yml` enforce different minimum ingest row-count thresholds for the same check

**File:** `.github/workflows/docs.yml:52`, `.github/workflows/ci.yml:74-85`
**Severity:** INFO

**Issue:** `docs.yml`'s "Verify ingest row count" step fails if `count < 100`. `ci.yml`'s equivalent step only asserts `count > 0`. Both consume the same `ci_setup.py`-generated 3000-row synthetic CSV, so this discrepancy is currently harmless, but the two workflows silently disagree on what "enough data" means for the same synthetic dataset, and only one of them (`docs.yml`) would actually catch a partially-truncated sample.

**Fix:** Align both thresholds (e.g. both `< 100`, or both reference a shared `MIN_CI_ROWS` env var) so a future change to `ci_setup.py`'s row count doesn't silently pass one workflow while a real regression is masked in the other.

---

### IN-04: Surrogate-key generation for the customer vs. product dimension is inconsistently located

**File:** `dbt_project/snapshots/dim_customers_snapshot.sql:15`, `dbt_project/models/intermediate/int_product_metrics.sql:37`, `dbt_project/snapshots/dim_products_snapshot.sql:15-16`
**Severity:** INFO

**Issue:** `customer_key` is generated inline inside the snapshot (`dim_customers_snapshot.sql:15`: `{{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key`), while `product_key` is generated one layer earlier, in the intermediate model (`int_product_metrics.sql:37`), and simply passed through by the snapshot (`dim_products_snapshot.sql:15-16`). Both are deterministic and correct, but the inconsistency in *where* the key is minted makes the two snapshot files harder to compare at a glance and is an easy trap for a future contributor copying one pattern into the other snapshot.

**Fix:** Pick one convention (recommend generating in the intermediate model for both, since that's the layer with the natural key already isolated) and apply it consistently.

---

## Structural Findings (fallow)

No structural pre-pass was provided for this review. All findings above are from direct file reading, cross-file tracing, and empirical verification (live `dbt build`/`dbt run` execution against a scratch DuckDB, and `pip install` against the pinned requirement files in isolated virtualenvs).

---

_Reviewed: 2026-07-03T15:00:00Z_
_Reviewer: Claude (adversarial code review, deep analysis)_
_Depth: deep (cross-module, 35 files, plus live execution verification)_
