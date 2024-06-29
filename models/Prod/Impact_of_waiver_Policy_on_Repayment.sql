
-- Analyze repayment behavior comparing cases with and without waiver
with repayment_analysis as (
    select
        case when `Waive Off Amount` > 0 then 'With Waiver' else 'Without Waiver' end as waiver_status,
        sum(`Total repay amount`) as total_repay_amount,
        count(*) as num_transactions
   from {{ ref('StgRaw') }}
    group by waiver_status  -- Group by the waiver_status column
)
select
    waiver_status,
    sum(total_repay_amount) as total_repay_amount,
    sum(num_transactions) as num_transactions,
    sum(total_repay_amount) / sum(num_transactions) as avg_repay_per_transaction
from repayment_analysis
group by waiver_status  -- Group by waiver_status again in the outer query
order by waiver_status
