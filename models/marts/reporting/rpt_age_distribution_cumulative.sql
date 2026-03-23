{{ config(
    materialized='incremental',
    database='traffic-offenders',
    schema='traffic'
) }}

SELECT *
FROM {{ ref('rpt_age_distribution') }} 

{% if is_incremental() %}
-- Prevent duplicate years
WHERE {{ env_var('DBT_YEAR') }} NOT IN (
    SELECT DISTINCT year FROM {{ this }}
)
{% endif %}