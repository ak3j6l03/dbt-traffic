with source as (
    select *
    from {{ source('processed', 'traffic_processed_2025') }}
),

cleaned as (
    select
        cast(violation_year_month as string) as violation_year_month,
        SAFE_CAST(age AS INT64) AS age,  
        cast(gender as string) as gender,
        cast(violation_type as string) as violation_type,
        cast(roc_year as int) as roc_year,
        cast(month as int) as month,
        cast(year as int) as year
    from source
)

select *
from cleaned
WHERE (age IS NULL OR age >= 0) 