{% snapshot dim_products_snapshot %}

{{
    config(
      target_database=target.database,
      target_schema=target.schema,
      unique_key='stock_code',

      strategy='check',
      check_cols=['description', 'min_price', 'max_price', 'avg_price', 'order_count', 'total_revenue'],
    )
}}

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
        avg_price
    FROM {{ ref('int_product_metrics') }}

{% endsnapshot %}
