WITH parsed_data AS (
    SELECT
        mac AS MAC_ID,
        TO_TIMESTAMP(CONCAT(eventdate, ' ', eventtime), 'YYYY-MM-DD HH24:MI:SS') AS eventdatetime,
        chname AS channel,
        program_id,
        geo_location,
        code,
        sat,
        ts,
        indextime,
        SPLIT_PART(geo_location, ',', 1)::DOUBLE PRECISION AS latitude,
        SPLIT_PART(geo_location, ',', 2)::DOUBLE PRECISION AS longitude
    FROM raw_viewership_data
),

minute_bucketed AS (
    SELECT
        *,
        DATE_TRUNC('minute', eventdatetime) AS minute_bucket
    FROM parsed_data
),

latest_event_per_minute AS (
    SELECT
        mac AS MAC_ID,
        minute_bucket,
        channel,
        program_id,
        latitude,
        longitude,
        code,
        sat,
        ts,
        indextime,
        ROW_NUMBER() OVER (PARTITION BY mac, minute_bucket ORDER BY eventdatetime DESC) AS row_num
    FROM minute_bucketed
),

filtered_data AS (
    SELECT
        *
    FROM latest_event_per_minute
    WHERE row_num = 1
),

filled_minutes AS (
    SELECT
        MAC_ID,
        channel,
        program_id,
        latitude,
        longitude,
        code,
        sat,
        ts,
        indextime,
        MINUTE_BUCKET AS event_start_time,
        LEAD(MINUTE_BUCKET) OVER (PARTITION BY MAC_ID ORDER BY MINUTE_BUCKET) AS event_end_time
    FROM filtered_data
),

minute_expanded AS (
    SELECT
        MAC_ID,
        channel,
        program_id,
        latitude,
        longitude,
        code,
        sat,
        ts,
        indextime,
        GENERATE_SERIES(event_start_time, COALESCE(event_end_time - INTERVAL '1 MINUTE', event_start_time), INTERVAL '1 MINUTE') AS minute
    FROM filled_minutes
),
enriched_viewership AS (
    SELECT
        me.MAC_ID,
        me.minute AS datetime,
        me.channel,
        me.program_id,
        me.latitude,
        me.longitude,
        me.code,
        me.sat,
        me.ts,
        me.indextime,
        pm.program_name,
        pm.genre
    FROM minute_expanded me
    LEFT JOIN program_mapping pm ON me.program_id = pm.program_id
)

SELECT *
FROM enriched_viewership;
