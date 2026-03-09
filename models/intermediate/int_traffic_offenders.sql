with traffic as (
    select *
    from {{ ref('stg_traffic_offenders') }}
),

violation_lookup as (
    select *
    from {{ ref('violation_type_lookup') }}
),

final as (
    select
        t.violation_year_month,
        t.age,
        case
            when t.gender = 'M' then 'Male'
            when t.gender = 'F' then 'Female'
            else null
        end as gender,
        v.repeat_violation_type as repeat_violation_type,
        t.roc_year,
        t.month,
        t.year
    from traffic t
    left join violation_lookup v
        on t.violation_type = v.violation_type
)

select *
from final