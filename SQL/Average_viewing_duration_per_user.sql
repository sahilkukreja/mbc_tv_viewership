WITH 
ViewershipDuration AS (
    SELECT 
        *,
        CASE 
            WHEN code = 'SSTAND' AND prev_datetime IS NOT NULL THEN
                (DATEDIFF(seconds, prev_datetime, datetime) / 60.0)
            WHEN code != 'SSTAND' AND next_datetime IS NOT NULL THEN
                DATEDIFF(seconds, datetime, next_datetime) / 60.0 
            ELSE 0
        END AS duration_minutes
    FROM (
        SELECT 
            *,
            LAG(datetime) OVER (PARTITION BY MAC_ID ORDER BY datetime) AS prev_datetime,
            LEAD(datetime) OVER (PARTITION BY MAC_ID ORDER BY datetime) AS next_datetime
        FROM 
            downstream
    )
),
UserViewingDuration AS (
    SELECT 
        date(eventdate) as date
        MAC_ID,
        SUM(duration_minutes) AS total_viewing_minutes
    FROM 
        ViewershipDuration
    GROUP BY 
        date(eventdate), MAC_ID
)
SELECT 
    date,
    SUM(total_viewing_minutes) / COUNT(distinct MAC_ID) AS average_viewing_duration_minutes
FROM 
    UserViewingDuration group by date;
