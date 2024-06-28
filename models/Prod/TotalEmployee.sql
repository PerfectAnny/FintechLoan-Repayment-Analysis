SELECT COUNT(DISTINCT StaffID) AS TotalEmployees
FROM   {{ ref('StgRaw') }} AS R
WHERE DATE(Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)


SELECT Date, StaffID
FROM  {{ ref('StgRaw') }} AS R
LIMIT 10

