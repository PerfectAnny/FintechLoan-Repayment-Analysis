-- Revenue and Losses Analysis
with repayment_data as (
    select
        parse_date('%Y%m%d', cast(Date as string)) as parsed_date,
        cast(`Total repay amount` as float64) as total_repay_amount,
        cast(`Total left unpaid principal` as float64) as total_left_unpaid_principal
    from {{ ref('StgRaw') }}
    where safe_cast(`Total repay amount` as float64) is not null
        and safe_cast(`Total left unpaid principal` as float64) is not null
),

monthly_revenue_and_losses as (
    select
        extract(year from parsed_date) as year,
        extract(month from parsed_date) as month,
        sum(total_repay_amount) as total_repaid_amount,
        sum(total_left_unpaid_principal) as total_left_unpaid_principal
    from repayment_data
    where parsed_date is not null
    group by year, month
)

select
    year,
    month,
    total_repaid_amount,
    total_left_unpaid_principal,
    total_repaid_amount as total_revenue,
    total_left_unpaid_principal as total_losses
from monthly_revenue_and_losses
order by year, month
