{% macro dayofweek_expression() %}
{% if target.type == 'snowflake' %} EXTRACT(dayofweekiso FROM date_day) {% else %} EXTRACT(isodow FROM date_day) {% endif %}
{% endmacro %}
