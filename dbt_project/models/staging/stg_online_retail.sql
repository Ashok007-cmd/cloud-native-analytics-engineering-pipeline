WITH source AS (
    SELECT
        TRIM(CAST(invoice_no AS VARCHAR)) AS invoice_no,
        TRIM(CAST(stock_code AS VARCHAR)) AS stock_code,
        TRIM(CAST(description AS VARCHAR)) AS description,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(invoice_date AS TIMESTAMP) AS invoice_date,
        TRY_CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
        COALESCE(
            NULLIF(TRIM(CAST(customer_id AS VARCHAR)), ''), '__missing'
        ) AS customer_id,
        TRIM(CAST(country AS VARCHAR)) AS country
    FROM {{ source('raw', 'online_retail') }}
    WHERE
        invoice_no IS NOT NULL
        AND invoice_no != ''
        AND stock_code IS NOT NULL
        AND stock_code != ''
        AND invoice_date IS NOT NULL
        AND unit_price IS NOT NULL
),

filtered AS (
    SELECT *
    FROM source
    WHERE unit_price > 0
),

deduped AS (
    SELECT
        invoice_no,
        stock_code,
        description,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country
    FROM (
        SELECT
            *,
            -- Keep the most complete/most recently-updated variant when the same
            -- (invoice_no, stock_code, invoice_date, customer_id) has multiple rows
            -- with differing description/country (e.g. mid-year description updates).
            -- Ordering by description length (longest = most complete) then
            -- description alphabetically gives a deterministic, meaningful tiebreak
            -- instead of an arbitrary one that silently drops legitimate variants.
            ROW_NUMBER() OVER (
                PARTITION BY invoice_no, stock_code, invoice_date, customer_id
                ORDER BY LENGTH(description) DESC, description ASC, country ASC
            ) AS row_num
        FROM filtered
    ) AS ranked
    WHERE row_num = 1
),

surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'invoice_no',
            'stock_code',
            'invoice_date',
            'customer_id',
        ]) }} AS order_item_id,
        invoice_no,
        stock_code,
        description,
        quantity,
        quantity < 0 AS is_return,
        invoice_date,
        CAST(invoice_date AS DATE) AS invoice_date_only,
        unit_price,
        customer_id,
        country
    FROM deduped
)

SELECT * FROM surrogate
