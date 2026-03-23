{{ config(
    materialized='table',
    cluster_by=['age']
) }}

SELECT
    age,
    year,
    COUNT(*) AS total_violations
FROM {{ ref('int_traffic_offenders') }}
WHERE age IS NOT NULL
GROUP BY age