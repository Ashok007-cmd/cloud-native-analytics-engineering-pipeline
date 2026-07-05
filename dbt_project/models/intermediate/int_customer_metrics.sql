{{ config(materialized='table') }}

WITH dataset_bounds AS (
    SELECT MAX(invoice_date) AS max_date
    FROM {{ ref('stg_online_retail') }}
),

customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice_no) AS total_orders,
        COUNT(*) AS total_items,
        SUM(quantity) AS total_quantity,
        SUM(quantity * unit_price) AS total_revenue,
        {{ calculate_gross_revenue() }} AS gross_revenue,
        COUNT(CASE WHEN is_return THEN 1 END) AS return_count,
        MIN(invoice_date) AS first_order_date,
        MAX(invoice_date) AS last_order_date
    FROM {{ ref('stg_online_retail') }}
    GROUP BY customer_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,
    c.customer_id,
    c.total_orders,
    c.total_items,
    c.total_quantity,
    ROUND(c.total_revenue, 2) AS total_revenue,
    ROUND(c.gross_revenue, 2) AS gross_revenue,
    c.return_count,
    c.first_order_date,
    c.last_order_date,
    {{ dbt.datediff("c.last_order_date", "b.max_date", "day") }}
        AS recency_days,
    CASE
        WHEN c.customer_id = '__missing' THEN 'unknown'
        WHEN
            c.total_orders >= {{ var('rfm_high_value_min_orders') }}
            AND c.gross_revenue >= {{ var('rfm_high_value_min_revenue') }}
            THEN 'high_value'
        WHEN
            c.total_orders >= {{ var('rfm_medium_value_min_orders') }}
            THEN 'medium_value'
        ELSE 'low_value'
    END AS customer_segment
FROM customer_orders AS c
CROSS JOIN dataset_bounds AS b
