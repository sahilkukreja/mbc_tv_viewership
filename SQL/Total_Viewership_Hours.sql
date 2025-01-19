WITH ViewershipDuration AS (
    SELECT 
        *,
        CASE 
            WHEN code = 'SSTAND' AND prev_datetime IS NOT NULL THEN
                (DATEDIFF(seconds, prev_datetime, datetime) / 60.0)
            WHEN code != 'SSTAND' AND next_datetime IS NOT NULL THEN
                LEAST(DATEDIFF(seconds, datetime, next_datetime) / 60.0, 10) 
            ELSE 0
        END AS duration_minutes
    FROM (
        SELECT 
            *,
            LAG(datetime) OVER (PARTITION BY MAC_ID ORDER BY datetime) AS prev_datetime,
            LEAD(datetime) OVER (PARTITION BY MAC_ID ORDER BY datetime) AS next_datetime
        FROM 
            minute_level_viewership
    )
)
SELECT 
    channel,
    DATE_TRUNC('day', datetime) AS date,
    SUM(duration_minutes) / 60 AS total_viewership_hours
FROM 
    ViewershipDuration
WHERE 
    channel IS NOT NULL AND channel != ''
GROUP BY 
    channel, DATE_TRUNC('day', datetime)
ORDER BY 
    date, total_viewership_hours DESC;
