-- Repayment Patterns and Trends
with repayment_data as (
    select
        `ticket category` as ticket_category,
        parse_date('%Y%m%d', cast(Date as string)) as parsed_date,
        cast(`Repay rate` as float64) as repay_rate,
        cast(`Total repay amount` as float64) as total_repay_amount
    from {{ ref('StgRaw') }}
    where safe_cast(`Repay rate` as float64) is not null 
        and safe_cast(`Total repay amount` as float64) is not null
),

monthly_repayment as (
    select
        extract(year from parsed_date) as year,
        extract(month from parsed_date) as month,
        ticket_category,
        avg(repay_rate) as avg_repay_rate,
        sum(total_repay_amount) as total_repay_amount
    from repayment_data
    where parsed_date is not null
    group by year, month, ticket_category
)

select
    year,
    month,
    ticket_category,
    avg_repay_rate,
    total_repay_amount
from monthly_repayment
order by year, month, ticket_category
