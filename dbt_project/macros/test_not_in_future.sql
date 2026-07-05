{% macro test_not_in_future(model, column_name) %}
    SELECT {{ adapter.quote(column_name) }}
    FROM {{ model }}
    WHERE {{ adapter.quote(column_name) }} > {{ dbt.current_timestamp() }}
{% endmacro %}
