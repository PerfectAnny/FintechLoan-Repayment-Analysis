-- Define a model to compare repayment rate per categories
with repayment_summary as (
    select
        `ticket category`,
        avg(`Repay rate`) as avg_repayment_rate
    from {{ ref('StgRaw') }}
    group by `ticket category`
)
select
    `ticket category`,
    avg_repayment_rate
from repayment_summary
order by avg_repayment_rate desc 
