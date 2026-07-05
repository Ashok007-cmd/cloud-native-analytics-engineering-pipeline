{#-
    Returns a SQL CASE expression that maps EXTRACT(MONTH FROM date_column)
    to its full English month name. Mirrors the pattern used by
    dayofweek_expression() so month-name mapping isn't hardcoded inline
    wherever it's needed (currently dim_dates.sql).
-#}
{% macro month_name_expression(date_column) %}
CASE EXTRACT(MONTH FROM {{ date_column }})
    WHEN 1 THEN 'January'
    WHEN 2 THEN 'February'
    WHEN 3 THEN 'March'
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
    WHEN 7 THEN 'July'
    WHEN 8 THEN 'August'
    WHEN 9 THEN 'September'
    WHEN 10 THEN 'October'
    WHEN 11 THEN 'November'
    WHEN 12 THEN 'December'
END
{% endmacro %}
