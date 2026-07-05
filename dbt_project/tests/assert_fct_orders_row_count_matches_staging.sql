-- Singular test: fct_orders must contain the same number of rows as stg_online_retail.
-- A mismatch greater than 0.1% indicates a join fan-out (duplicate rows from SCD2
-- or missing dedup logic) or a data loss bug upstream.
-- Uses integer arithmetic to avoid floating-point precision edge cases at scale (>2^53 rows).
-- Returns rows (i.e. fails) when the deviation exceeds 0.1%.
WITH staging AS (
    SELECT COUNT(*) AS n FROM {{ ref('stg_online_retail') }}
),
fact AS (
    SELECT COUNT(*) AS n FROM {{ ref('fct_orders') }}
)
SELECT 1
FROM staging, fact
WHERE ABS(CAST(fact.n AS BIGINT) - CAST(staging.n AS BIGINT))
      > CAST(0.001 * CAST(staging.n AS NUMERIC(38,0)) AS BIGINT)
