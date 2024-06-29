-- Aggregate repayment rates per month
WITH monthly_repayment_rates AS (
    SELECT
        DATE(PARSE_DATE('%Y%m%d', CAST(Date AS STRING))) AS date_parsed,
        AVG(`Repay rate`) AS avg_repay_rate
    FROM {{ ref('StgRaw') }}
    GROUP BY date_parsed
)
SELECT
    EXTRACT(YEAR FROM date_parsed) AS year,
    EXTRACT(MONTH FROM date_parsed) AS month,
    avg_repay_rate
FROM monthly_repayment_rates
ORDER BY year, month
