{{ config(
    materialized='table',
    partition_by={
        "field": "violation_date",
        "data_type": "date"
    }
) }}

SELECT
    violation_year_month,
    PARSE_DATE('%Y-%m', violation_year_month) AS violation_date,
    COUNT(*) AS total_violations
FROM {{ ref('int_traffic_offenders') }}
GROUP BY violation_year_month, violation_date