SELECT COUNT(DISTINCT StaffID) AS TotalEmployees
FROM   {{ ref('StgRaw') }} AS R
WHERE DATE(PARSE_DATE('%Y%m%d', CAST(Date AS STRING))) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)





