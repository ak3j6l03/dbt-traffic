{{ config(
    materialized='table',
    cluster_by=['repeat_violation_type']
) }}

SELECT
    repeat_violation_type,
    year,
    COUNT(*) AS total_violations
FROM {{ ref('int_traffic_offenders') }}
GROUP BY repeat_violation_type, year