{{ config(
    materialized='incremental',
    schema="{{ env_var('DBT_DATASET') }}"
    --unique_key=['repeat_violation_type','year']
) }}

SELECT *
FROM {{ ref('rpt_violation_type') }} 

{% if is_incremental() %}
-- Prevent duplicate years
WHERE {{ env_var('DBT_YEAR') }} NOT IN (
    SELECT DISTINCT year FROM {{ this }}
)
{% endif %}