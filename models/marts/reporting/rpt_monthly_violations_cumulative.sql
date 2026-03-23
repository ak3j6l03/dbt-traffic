{{ config(
    materialized='incremental',
    schema="{{ env_var('DBT_DATASET') }}"
    --unique_key=['violation_year_month','year']
) }}

SELECT *
FROM {{ ref('rpt_monthly_violations') }} 

{% if is_incremental() %}
-- Prevent duplicate years
WHERE {{ env_var('DBT_YEAR') }} NOT IN (
    SELECT DISTINCT year FROM {{ this }}
)
{% endif %}