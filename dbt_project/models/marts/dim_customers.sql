{{
    config(
        materialized='table',
        cluster_by='customer_segment'
    )
}}

-- Current-state customer dimension.
-- Wraps dim_customers_snapshot (SCD Type 2) and returns only the latest record
-- per customer for easy BI consumption without SCD2 awareness in downstream tools.
SELECT
    customer_key,
    customer_id,
    total_orders,
    total_items,
    total_quantity,
    total_revenue,
    gross_revenue,
    return_count,
    first_order_date,
    last_order_date,
    recency_days,
    customer_segment,
    dbt_updated_at AS snapshot_updated_at
FROM {{ ref('dim_customers_snapshot') }}
WHERE dbt_valid_to IS NULL
