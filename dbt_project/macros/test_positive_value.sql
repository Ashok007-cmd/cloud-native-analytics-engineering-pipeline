{% macro test_positive_value(model, column_name) %}
    SELECT {{ adapter.quote(column_name) }}
    FROM {{ model }}
    WHERE {{ adapter.quote(column_name) }} <= 0
       OR {{ adapter.quote(column_name) }} IS NULL
{% endmacro %}
