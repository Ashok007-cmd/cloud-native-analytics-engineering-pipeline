{%- set date_bounds_query -%}
    SELECT
        CAST(MIN(invoice_date) AS DATE) - INTERVAL '7 days' AS min_date,
        CAST(MAX(invoice_date) AS DATE) + INTERVAL '7 days' AS max_date
    FROM {{ ref('stg_online_retail') }}
{%- endset -%}

{#
    Hardcoded fallback covers the Online Retail II dataset (2009-2011). Only
    used when stg_online_retail does not exist yet (e.g. a fresh project's
    first `dbt build`, before staging has run) — there is no data to derive
    bounds from in that case, so a static fallback is the correct behavior.
    If the relation DOES exist but the bounds query returns nothing (e.g. an
    empty staging table), that's a real data problem, so raise instead of
    silently reusing the stale hardcoded dates.
#}
{%- set min_date = '2009-12-01' -%}
{%- set max_date = '2012-01-01' -%}

{%- set stg_relation = load_relation(ref('stg_online_retail')) -%}
{%- if execute and stg_relation is not none -%}
    {%- set results = run_query(date_bounds_query) -%}
    {%- if results and results.columns[0].values()[0] -%}
        {%- set min_date = results.columns[0].values()[0] | string -%}
        {%- set max_date = results.columns[1].values()[0] | string -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "Could not determine date bounds from stg_online_retail. "
            "The relation exists but returned no rows — ensure source data "
            "has been ingested before building dim_dates."
        ) }}
    {%- endif -%}
{%- endif -%}

WITH filtered_spine AS (
    {{ dbt.date_spine(
        datepart="day",
        start_date="CAST('" ~ min_date ~ "' AS DATE)",
        end_date="CAST('" ~ max_date ~ "' AS DATE)"
    ) }}
),

date_dim AS (
    SELECT
        {% if target.type == 'snowflake' %}
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
        WEEKOFYEAR(date_day) AS week_of_year,
        {% else %}
            CAST(STRFTIME(date_day, '%Y%m%d') AS INTEGER) AS date_key,
            CAST(EXTRACT('week' FROM date_day) AS INTEGER) AS week_of_year,
        {% endif %}
        date_day AS full_date,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        CAST(EXTRACT(DOY FROM date_day) AS INTEGER) AS day_of_year,
        CASE {{ dayofweek_expression() }}
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END AS day_name,
        {{ month_name_expression('date_day') }} AS month_name,
        {{ dayofweek_expression() }} IN (6, 7) AS is_weekend,
        NOT({{ dayofweek_expression() }} IN (6, 7)) AS is_weekday
    FROM filtered_spine
)

SELECT * FROM date_dim
