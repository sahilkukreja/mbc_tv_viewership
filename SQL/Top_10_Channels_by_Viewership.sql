WITH RankedChannels AS (
    SELECT 
        channel,
        date,
        total_viewership_hours,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('day', datetime) ORDER BY total_viewership_hours DESC) AS rank
    FROM 
        Total_Viewership_Hours
    WHERE 
        channel IS NOT NULL AND channel != ''
)
SELECT 
    channel,
    date,
    total_viewership_hours,
    rank
FROM 
    RankedChannels
WHERE 
    rank <= 10;
