-- Singular test: high_value customers must have positive gross_revenue.
-- A customer with negative gross_revenue should never be classified as high_value
-- because the RFM threshold requires both order count AND revenue minimums.
-- Returns rows (fails) if any such misclassification exists.
SELECT customer_id, customer_segment, gross_revenue
FROM {{ ref('int_customer_metrics') }}
WHERE customer_segment = 'high_value'
  AND gross_revenue <= 0
