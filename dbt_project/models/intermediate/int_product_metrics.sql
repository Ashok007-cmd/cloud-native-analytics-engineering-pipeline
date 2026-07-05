WITH product_stats AS (
    SELECT
        stock_code,
        COUNT(DISTINCT invoice_no) AS order_count,
        SUM(quantity) AS total_quantity_sold,
        SUM(quantity * unit_price) AS total_revenue,
        {{ calculate_gross_revenue() }}                                     AS gross_revenue,
        COUNT(CASE WHEN is_return THEN 1 END) AS return_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        MIN(unit_price) AS min_price,
        MAX(unit_price) AS max_price,
        ROUND(AVG(unit_price), 2) AS avg_price
    FROM {{ ref('stg_online_retail') }}
    GROUP BY stock_code
),

desc_counts AS (
    SELECT
        stock_code,
        description,
        COUNT(*) AS cnt
    FROM {{ ref('stg_online_retail') }}
    GROUP BY stock_code, description
),

ranked_desc AS (
    SELECT DISTINCT
        stock_code,
        FIRST_VALUE(description) OVER (
            PARTITION BY stock_code
            ORDER BY cnt DESC, description ASC
        ) AS description
    FROM desc_counts
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.stock_code']) }} AS product_key,
    p.stock_code,
    COALESCE(d.description, 'Unknown') AS description,
    p.order_count,
    p.total_quantity_sold,
    ROUND(p.total_revenue, 2) AS total_revenue,
    ROUND(p.gross_revenue, 2) AS gross_revenue,
    p.return_count,
    p.unique_customers,
    ROUND(p.min_price, 2) AS min_price,
    ROUND(p.max_price, 2) AS max_price,
    p.avg_price
FROM product_stats AS p
LEFT JOIN ranked_desc AS d ON p.stock_code = d.stock_code
