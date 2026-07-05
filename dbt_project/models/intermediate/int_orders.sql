SELECT
    {{ dbt_utils.generate_surrogate_key(['invoice_no', 'customer_id']) }} AS order_key,
    invoice_no,
    customer_id,
    MIN(invoice_date) AS order_date,
    COUNT(*) AS line_item_count,
    COUNT(DISTINCT stock_code) AS unique_items,
    SUM(quantity) AS total_quantity,
    SUM(CASE WHEN is_return THEN 0 ELSE 1 END) AS forward_item_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_order_value,
    ROUND({{ calculate_gross_revenue() }}, 2) AS forward_order_value,
    BOOL_OR(is_return) AS has_returns
FROM {{ ref('stg_online_retail') }}
GROUP BY invoice_no, customer_id
