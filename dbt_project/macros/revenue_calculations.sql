{% macro calculate_gross_revenue(quantity_col='quantity', price_col='unit_price', is_return_col='is_return') %}
    SUM(CASE WHEN NOT {{ is_return_col }} THEN {{ quantity_col }} * {{ price_col }} ELSE 0 END)
{% endmacro %}
