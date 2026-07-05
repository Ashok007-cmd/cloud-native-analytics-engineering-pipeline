{# For Snowflake production: clustering on (date_key, customer_key) improves
   range-scan performance on time-range + customer queries. Weekly
   reclustering is automated via the RECLUSTER_FCT_ORDERS Snowflake TASK
   created by scripts/snowflake_bootstrap.sql — see that file for the
   schedule. DuckDB ignores cluster_by (Snowflake-only materialization
   option). #}
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='fail',
        cluster_by=['date_key', 'customer_key'],
        post_hook=[
            "{% if is_incremental() and target.type == 'duckdb' %}
                -- Defense-in-depth only: dbt-duckdb's DEFAULT incremental strategy for
                -- this model (unset incremental_strategy resolves to
                -- get_incremental_delete_insert_sql) already performs an atomic,
                -- transactional delete+insert dedup on unique_key, so this post-hook is
                -- a no-op under current adapter defaults. It exists to guard against a
                -- future incremental_strategy override (e.g. 'append' or 'merge')
                -- silently reintroducing duplicates. If it ever does have work to do,
                -- ranking by rowid (in addition to invoice_date) as a deterministic
                -- tiebreaker eliminates the stalemate where two rows share both
                -- order_item_id AND invoice_date: a MAX(invoice_date)-only comparison
                -- would match both rows and delete neither, letting the duplicate
                -- persist silently. rowid guarantees a strict total order, so exactly
                -- one row per order_item_id is always kept.
                DELETE FROM {{ this }}
                WHERE rowid NOT IN (
                    SELECT rowid
                    FROM (
                        SELECT
                            rowid,
                            order_item_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY order_item_id
                                ORDER BY invoice_date DESC, rowid DESC
                            ) AS rn
                        FROM {{ this }}
                    ) AS ranked
                    WHERE rn = 1
                );
            {% endif %}"
        ]
    )
}}

WITH staging AS (
    SELECT
        order_item_id,
        invoice_no,
        stock_code,
        customer_id,
        quantity,
        is_return,
        unit_price,
        invoice_date,
        invoice_date_only,
        country
    FROM {{ ref('stg_online_retail') }}
),

-- Point-in-time SCD2 joins: each staging row is matched to the dimension
-- version that was valid AT THE TIME the invoice was recorded, not the
-- current version. Since dbt_valid_from/dbt_valid_to ranges are
-- non-overlapping per customer_id (or stock_code), each staging row matches
-- at most one dimension row, avoiding both fan-out duplication and
-- misattributing historical orders to the customer's/product's current state.
-- KNOWN LIMITATION: once a fact row is written, its customer_key/product_key
-- reflect the dimension version that was open AT INSERT TIME and are never
-- retroactively re-joined if a later snapshot run closes that version with a
-- boundary at or before the fact row's invoice_date (the classic late-arriving
-- dimension gap). Mitigate by re-running `dbt build --full-refresh --select
-- fct_orders` after any dimension backfill, or by adding a dimension-change-
-- driven reprocessing window if this becomes a live concern.
-- The earliest snapshot row per customer/product has its dbt_valid_from
-- floored to the epoch: a "check" strategy snapshot's first-ever run stamps
-- dbt_valid_from as the run timestamp, not any real historical date, so a
-- literal dbt_valid_from lower bound would leave every order that predates
-- the snapshot's first run (e.g. a historical backfill) unmatched. Treating
-- the earliest known version as valid "since the beginning of time" lets
-- historical orders attach to the best available (earliest) version instead
-- of falling back to the -1 default key.
customers AS (
    SELECT
        customer_key,
        customer_id,
        CASE
            WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY customer_id)
                THEN CAST('1900-01-01' AS TIMESTAMP)
            ELSE dbt_valid_from
        END AS dbt_valid_from,
        COALESCE(dbt_valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS dbt_valid_to
    FROM {{ ref('dim_customers_snapshot') }}
),

products AS (
    SELECT
        product_key,
        stock_code,
        CASE
            WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY stock_code)
                THEN CAST('1900-01-01' AS TIMESTAMP)
            ELSE dbt_valid_from
        END AS dbt_valid_from,
        COALESCE(dbt_valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS dbt_valid_to
    FROM {{ ref('dim_products_snapshot') }}
),

dates AS (
    SELECT
        date_key,
        full_date
    FROM {{ ref('dim_dates') }}
)

SELECT
    staging.order_item_id,
    COALESCE(customers.customer_key, '-1') AS customer_key,
    COALESCE(products.product_key, '-1') AS product_key,
    COALESCE(dates.date_key, 19000101) AS date_key,
    staging.invoice_date,
    staging.invoice_no,
    staging.is_return,
    staging.quantity,
    staging.unit_price,
    ROUND(staging.quantity * staging.unit_price, 2) AS revenue,
    staging.country
FROM staging
LEFT JOIN customers
    ON
        staging.customer_id = customers.customer_id
        AND staging.invoice_date >= customers.dbt_valid_from
        AND staging.invoice_date < customers.dbt_valid_to
LEFT JOIN products
    ON
        staging.stock_code = products.stock_code
        AND staging.invoice_date >= products.dbt_valid_from
        AND staging.invoice_date < products.dbt_valid_to
LEFT JOIN dates ON staging.invoice_date_only = dates.full_date
{% if is_incremental() %}
-- 3-day lookback absorbs late-arriving invoices (typical 1-2 business day lag)
-- and corrects return records that share the original invoice date
WHERE
    staging.invoice_date >= (
        SELECT
            COALESCE(
                MAX(invoice_date), CAST('1900-01-01' AS TIMESTAMP)
            ) - INTERVAL '3 days'
        FROM {{ this }}
    )
{% endif %}
