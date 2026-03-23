{{ config(
    materialized='incremental',
    schema="{{ env_var('DBT_DATASET') }}"
    # unique_key=['gender','year']
) }}

SELECT *
FROM {{ ref('rpt_gender_distribution') }} 

{% if is_incremental() %}
-- Prevent duplicate years
WHERE {{ env_var('DBT_YEAR') }} NOT IN (
    SELECT DISTINCT year FROM {{ this }}
)
{% endif %}