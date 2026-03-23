{{ config(
    materialized='table',
    cluster_by=['gender']
) }}

SELECT
    gender,
    year,
    COUNT(*) AS total_violations
FROM {{ ref('int_traffic_offenders') }}
GROUP BY gender, year