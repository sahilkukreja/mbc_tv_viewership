WITH daily_viewership AS (
    
    SELECT 
        DATE(datetime) AS view_date,
        COUNT(DISTINCT MAC_ID) AS total_viewers
    FROM minute_level_viewership
    GROUP BY DATE(datetime)
),

channel_viewership AS (
    SELECT 
        chname AS channel,
        DATE(datetime) AS view_date,
        COUNT(DISTINCT MAC_ID) AS unique_viewers_per_channel
    FROM minute_level_viewership
    GROUP BY chname, DATE(datetime)
),

minute_ratings AS (
    SELECT 
        chname AS channel,
        DATE(datetime) AS view_date,
        datetime AS minute_bucket,
        COUNT(DISTINCT MAC_ID) AS unique_viewers_per_minute
    FROM minute_level_viewership
    GROUP BY chname, DATE(datetime), datetime
),
trp_calculation AS (
    SELECT 
        m.channel,
        m.view_date,
        SUM(m.unique_viewers_per_minute) AS trp
    FROM minute_ratings m
    GROUP BY m.channel, m.view_date
),

reach_and_trp AS (
    SELECT 
        c.channel,
        c.view_date,
        c.unique_viewers_per_channel / d.total_viewers AS reach,
        t.trp AS total_trp
    FROM channel_viewership c
    JOIN daily_viewership d
        ON c.view_date = d.view_date
    JOIN trp_calculation t
        ON c.channel = t.channel AND c.view_date = t.view_date
)
SELECT 
    channel,
    view_date,
    ROUND(reach,2 ) AS reach,
    total_trp AS trp
FROM reach_and_trp
ORDER BY view_date, channel;
