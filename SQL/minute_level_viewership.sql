WITH EnrichedEvents AS (
    SELECT 
        *,
        DATE_TRUNC('minute', eventdatetime) AS minute_bucket 
    FROM 
        raw_data
),
RankedEvents AS (
    SELECT
        mac AS MAC_ID,
        minute_bucket,
        chname AS channel,
        program_name,
        code,
        ROW_NUMBER() OVER (PARTITION BY mac, minute_bucket ORDER BY eventtime DESC) AS row_num
    FROM 
        EnrichedEvents
)
SELECT
    MAC_ID,
    minute_bucket AS datetime,
    channel,
    program_name,
    code
FROM 
    RankedEvents
WHERE 
    row_num = 1;
