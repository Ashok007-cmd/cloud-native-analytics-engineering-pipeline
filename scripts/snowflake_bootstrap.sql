-- scripts/snowflake_bootstrap.sql
-- One-time Snowflake account setup for production. Run manually via
-- SnowSQL or the Snowflake worksheet UI, authenticated as ACCOUNTADMIN
-- (or a role with CREATE WAREHOUSE / CREATE ROLE / CREATE RESOURCE MONITOR
-- privileges). Not run by any CI/CD pipeline — Claude Code cannot execute
-- this against a live account; a human with account admin access must.
--
-- After running this script, set the following as GitHub repository
-- secrets (see docs/PRODUCTION_SETUP.md for exact `gh secret set` commands):
--   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
--   SNOWFLAKE_ROLE=DBT_ROLE, SNOWFLAKE_DATABASE=ONLINE_RETAIL_DB,
--   SNOWFLAKE_WAREHOUSE=ELT_WH
-- and the repository variable SNOWFLAKE_CI_ENABLED=true.

USE ROLE ACCOUNTADMIN;

-- ── Warehouse: safe cost defaults ───────────────────────────────────────────
-- X-Small, single-cluster, 5-minute auto-suspend. Matches PRD-05's stated
-- cost baseline. Right-sizing / multi-cluster tuning is Phase 10 (CST-01),
-- not this script — start minimal, scale up only if query queueing is
-- observed.
CREATE WAREHOUSE IF NOT EXISTS elt_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'dbt build/test compute for online_retail_pipeline';

-- ── Resource monitor: budget alert ──────────────────────────────────────────
-- Adjust CREDIT_QUOTA to your actual monthly budget (default assumes a
-- Snowflake trial's $400 credit allotment spread conservatively). Requires
-- NOTIFY_USERS to reference real Snowflake usernames who should receive
-- the threshold emails — update the placeholder below before running.
CREATE RESOURCE MONITOR IF NOT EXISTS elt_monthly_budget
    WITH
        CREDIT_QUOTA = 100
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 75 PERCENT DO NOTIFY
            ON 90 PERCENT DO NOTIFY
            ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE elt_wh SET RESOURCE_MONITOR = elt_monthly_budget;

-- TODO (human): set real notification recipients before relying on this —
-- resource monitors only email account admins by default. To notify
-- additional users, grant them the MONITOR privilege on this resource
-- monitor or configure a notification integration; see:
-- https://docs.snowflake.com/en/user-guide/resource-monitors

-- ── Database ─────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS online_retail_db
    COMMENT = 'Cloud-Native Analytics Engineering Pipeline — production';

CREATE SCHEMA IF NOT EXISTS online_retail_db.analytics;
CREATE SCHEMA IF NOT EXISTS online_retail_db.raw;

-- ── Roles: least-privilege, replaces the earlier GRANT ALL pattern ────────
CREATE ROLE IF NOT EXISTS dbt_role
    COMMENT = 'Used by CI/CD to build and test the dbt project. Read/write on analytics + raw schemas only — not GRANT ALL.';

CREATE ROLE IF NOT EXISTS read_only
    COMMENT = 'Analyst-facing role: SELECT-only access to the analytics schema for BI tools and ad-hoc queries.';

-- dbt_role: build/write access scoped to this database's schemas
GRANT USAGE ON WAREHOUSE elt_wh TO ROLE dbt_role;
GRANT USAGE ON DATABASE online_retail_db TO ROLE dbt_role;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA online_retail_db.analytics TO ROLE dbt_role;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA online_retail_db.raw TO ROLE dbt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA online_retail_db.analytics TO ROLE dbt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA online_retail_db.raw TO ROLE dbt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA online_retail_db.analytics TO ROLE dbt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA online_retail_db.raw TO ROLE dbt_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA online_retail_db.analytics TO ROLE dbt_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA online_retail_db.analytics TO ROLE dbt_role;
-- Weekly reclustering task (below) executes as this role.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE dbt_role;

-- read_only: SELECT-only on the analytics (marts-facing) schema. No access
-- to raw — analysts should never see unprocessed source data.
GRANT USAGE ON WAREHOUSE elt_wh TO ROLE read_only;
GRANT USAGE ON DATABASE online_retail_db TO ROLE read_only;
GRANT USAGE ON SCHEMA online_retail_db.analytics TO ROLE read_only;
GRANT SELECT ON ALL TABLES IN SCHEMA online_retail_db.analytics TO ROLE read_only;
GRANT SELECT ON FUTURE TABLES IN SCHEMA online_retail_db.analytics TO ROLE read_only;
GRANT SELECT ON ALL VIEWS IN SCHEMA online_retail_db.analytics TO ROLE read_only;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA online_retail_db.analytics TO ROLE read_only;

-- Assign dbt_role to the CI/CD service user. Replace <CI_SERVICE_USER> with
-- the Snowflake username created for GitHub Actions (do not reuse a human
-- login — create a dedicated service account).
-- GRANT ROLE dbt_role TO USER <CI_SERVICE_USER>;

-- Assign read_only to each analyst's Snowflake user, e.g.:
-- GRANT ROLE read_only TO USER <ANALYST_USER>;

-- ── Weekly reclustering task ─────────────────────────────────────────────
-- fct_orders is clustered on (date_key, customer_key) — see
-- dbt_project/models/marts/fct_orders.sql. Automatic clustering reclusters
-- continuously by default when a table has a clustering key, which is
-- usually sufficient and is the Snowflake-recommended default; this task
-- is a defense-in-depth manual RECLUSTER for cases where automatic
-- clustering falls behind (e.g. after a large backfill). Suspended by
-- default — resume it only if you've confirmed automatic clustering
-- credit consumption isn't already handling this table adequately
-- (check SYSTEM$CLUSTERING_INFORMATION / ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY).
USE ROLE dbt_role;
USE DATABASE online_retail_db;
USE SCHEMA analytics;

CREATE TASK IF NOT EXISTS recluster_fct_orders
    WAREHOUSE = elt_wh
    SCHEDULE = 'USING CRON 0 3 * * 1 UTC'  -- Monday 03:00 UTC
    COMMENT = 'Manual weekly RECLUSTER for fct_orders as a backstop to automatic clustering.'
AS
    ALTER TABLE fct_orders RECLUSTER;

-- Tasks are created SUSPENDED by default. Uncomment to activate once
-- automatic clustering has been evaluated:
-- ALTER TASK recluster_fct_orders RESUME;
