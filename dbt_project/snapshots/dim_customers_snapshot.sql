{% snapshot dim_customers_snapshot %}

{{
    config(
      target_database=target.database,
      target_schema=target.schema,
      unique_key='customer_id',

      strategy='check',
      check_cols=['customer_segment', 'total_orders', 'total_revenue', 'gross_revenue', 'return_count', 'total_items', 'total_quantity', 'recency_days'],
    )
}}

    SELECT
        customer_key,
        customer_id,
        total_orders,
        total_revenue,
        gross_revenue,
        return_count,
        total_items,
        total_quantity,
        first_order_date,
        last_order_date,
        recency_days,
        customer_segment
    FROM {{ ref('int_customer_metrics') }}

{% endsnapshot %}
