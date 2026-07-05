{{
    config(
        materialized='table',
        cluster_by='stock_code'
    )
}}

-- Current-state product dimension.
-- Wraps dim_products_snapshot (SCD Type 2) and returns only the latest record
-- per product for easy BI consumption without SCD2 awareness in downstream tools.
SELECT
    product_key,
    stock_code,
    description,
    order_count,
    total_quantity_sold,
    total_revenue,
    gross_revenue,
    return_count,
    unique_customers,
    min_price,
    max_price,
    avg_price,
    dbt_updated_at AS snapshot_updated_at
FROM {{ ref('dim_products_snapshot') }}
WHERE dbt_valid_to IS NULL
