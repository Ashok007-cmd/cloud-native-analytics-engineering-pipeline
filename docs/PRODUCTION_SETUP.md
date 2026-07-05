# Production Setup Runbook (Phase 7: Production Hardening)

This is the exact, step-by-step manual runbook for taking this pipeline from
"MVP on DuckDB" to "running against a real Snowflake account with CD active."
Every step here requires an action outside this repository — an external
signup, a live account credential, or an admin-permission API call — and
cannot be performed by an automated coding agent. Code changes needed to
*support* this setup have already been made; this document only covers the
remaining manual steps.

## 1. Create a Snowflake account

1. Sign up for a Snowflake trial at https://signup.snowflake.com (or use an
   existing account). Note the account identifier shown after signup
   (format: `<orgname>-<account_name>` or the legacy `<account_locator>`).
2. Log in to the Snowsight worksheet UI (or install SnowSQL) as the
   `ACCOUNTADMIN` role (or a role with `CREATE WAREHOUSE` / `CREATE ROLE` /
   `CREATE RESOURCE MONITOR` / `CREATE DATABASE` privileges).

## 2. Run the bootstrap SQL

1. Open `scripts/snowflake_bootstrap.sql` in this repo.
2. Before running, edit the `TODO (human)` line to set real resource-monitor
   notification recipients, and decide on a `CREDIT_QUOTA` matching your
   actual monthly budget (default: 100 credits/month — adjust to your
   trial's `$400` allotment or your paid plan's budget).
3. Paste the full script into a Snowsight worksheet (or run via
   `snowsql -f scripts/snowflake_bootstrap.sql`) and execute it. This
   creates:
   - Warehouse `ELT_WH` (X-Small, auto-suspend 5 min, auto-resume)
   - Resource monitor `ELT_MONTHLY_BUDGET` (75%/90% alert, 100% suspend)
   - Database `ONLINE_RETAIL_DB` with `analytics` and `raw` schemas
   - Role `DBT_ROLE` (CI/CD build/write access, least-privilege — not
     `GRANT ALL`)
   - Role `READ_ONLY` (analyst SELECT-only access to `analytics`)
   - Task `RECLUSTER_FCT_ORDERS` (weekly reclustering backstop, created
     suspended — leave suspended unless you've confirmed Snowflake's
     automatic clustering isn't already handling `fct_orders` adequately)
4. Create a dedicated Snowflake service user for CI/CD (do not reuse a
   human login):
   ```sql
   CREATE USER IF NOT EXISTS gh_actions_dbt
       PASSWORD = '<generate a strong random password>'
       DEFAULT_ROLE = dbt_role
       DEFAULT_WAREHOUSE = elt_wh
       MUST_CHANGE_PASSWORD = FALSE;
   GRANT ROLE dbt_role TO USER gh_actions_dbt;
   ```
5. For each analyst who needs read access:
   ```sql
   GRANT ROLE read_only TO USER <analyst_snowflake_username>;
   ```

## 3. Configure GitHub repository secrets and variables

Run these with the [GitHub CLI](https://cli.github.com/) (`gh`) from the
repo root, authenticated as an account with admin access to this repository
— **or** set the same values via the GitHub web UI under
**Settings → Secrets and variables → Actions**.

```bash
gh secret set SNOWFLAKE_ACCOUNT       # paste your account identifier
gh secret set SNOWFLAKE_USER          # gh_actions_dbt
gh secret set SNOWFLAKE_PASSWORD      # the password set in step 2.4
gh secret set SNOWFLAKE_ROLE          # DBT_ROLE
gh secret set SNOWFLAKE_DATABASE      # ONLINE_RETAIL_DB
gh secret set SNOWFLAKE_WAREHOUSE     # ELT_WH

gh variable set SNOWFLAKE_CI_ENABLED --body "true"
```

Each `gh secret set NAME` without a value prompts interactively — you can
also pipe a value in non-interactively, e.g.
`echo -n "$VALUE" | gh secret set SNOWFLAKE_PASSWORD`, but prefer the
interactive prompt for secrets so the value never touches shell history.

> **Note:** `.github/workflows/cd.yml` and `.github/workflows/ci.yml` gate
> Snowflake deployment/testing solely on `vars.SNOWFLAKE_CI_ENABLED ==
> 'true'` (an earlier hardcoded `repository_owner == 'ak'` check was removed
> since it would silently no-op if this repo is pushed under a different
> account/org — verify this is still the case if you're reading an older
> checkout).

## 4. Verify CD activates

1. Push a trivial change to `main` (or merge a PR).
2. Watch the `CD Pipeline` workflow run in the Actions tab. The `deploy` job
   should run (not `deploy-skipped`), reach "Validate Snowflake connection"
   (`dbt debug`), and complete "Run dbt build on Snowflake."
3. If `deploy-skipped` still runs: check that `SNOWFLAKE_CI_ENABLED` is set
   as a repository **variable** (not secret) with value exactly `true`.
4. If `dbt debug` fails: re-check the six `SNOWFLAKE_*` secrets for typos —
   the most common failure is an account identifier missing the region
   suffix (e.g. use `abc12345.us-east-1` not just `abc12345` if your account
   isn't in a default region).

## 5. Production Airflow deployment

This project's production Airflow deployment model is **the same
`docker-compose.yaml` used for local development, pointed at production
credentials** — there is no separate Kubernetes/ECS/cloud-native deployment
target. This is an intentional scope decision for this project's size, not
an oversight (see `.planning/phases/07-hardening/phase.md`); revisit if/when
scale requires a managed orchestrator.

1. Provision a host you control (VM, cloud instance, on-prem server) with
   Docker + Docker Compose installed.
2. Copy `airflow/.env.example` to `airflow/.env` on that host and fill in
   every value — **do not commit this file** (already gitignored). Generate
   the Fernet key and admin password using the commands in the file's
   header comments, and fill in the same `SNOWFLAKE_*` values used in step 3
   (with `DBT_PROFILE=snowflake DBT_TARGET=prod`).
3. From `airflow/` on that host: `docker compose up -d`.
4. Confirm the `dbt_cosmos_dag` DAG appears in the Airflow UI and a manual
   trigger completes successfully against Snowflake before enabling its
   schedule.

## 6. Post-setup checklist

- [ ] `scripts/snowflake_bootstrap.sql` executed against the live account
- [ ] Resource monitor notification recipients set to real users (not the
      placeholder in the script)
- [ ] Dedicated `gh_actions_dbt` service user created (not a human login)
- [ ] All 6 `SNOWFLAKE_*` secrets + `SNOWFLAKE_CI_ENABLED` variable set in
      GitHub
- [ ] CD pipeline run observed to reach and pass "Run dbt build on
      Snowflake"
- [ ] Production Airflow host provisioned and `docker compose up -d`
      running the Cosmos DAG against Snowflake
- [ ] Each analyst granted `READ_ONLY`, confirmed they can query
      `analytics.*` tables and cannot see `raw.*`
