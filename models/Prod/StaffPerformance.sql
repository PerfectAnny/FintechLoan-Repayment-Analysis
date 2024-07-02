-- Staff Performance Insights
with staff_performance_data as (
    select
        StaffID,
        Role,
        parse_date('%Y%m%d', cast(Date as string)) as parsed_date,
        cast(`Repay rate` as float64) as repay_rate,
        cast(`Total repay amount` as float64) as total_repay_amount,
        cast(`Handle num` as int64) as handle_num,
        cast(`Complete num` as int64) as complete_num
    from {{ ref('StgRaw') }}
    where safe_cast(`Repay rate` as float64) is not null 
        and safe_cast(`Total repay amount` as float64) is not null
        and safe_cast(`Handle num` as int64) is not null
        and safe_cast(`Complete num` as int64) is not null
),

staff_monthly_performance as (
    select
        StaffID,
        Role,
        extract(year from parsed_date) as year,
        extract(month from parsed_date) as month,
        avg(repay_rate) as avg_repay_rate,
        sum(total_repay_amount) as total_repay_amount,
        sum(handle_num) as total_handle_num,
        sum(complete_num) as total_complete_num
    from staff_performance_data
    where parsed_date is not null
    group by StaffID, Role, year, month
)

select
    StaffID,
    Role,
    year,
    month,
    avg_repay_rate,
    total_repay_amount,
    total_handle_num,
    total_complete_num
from staff_monthly_performance
order by StaffID, year, month
