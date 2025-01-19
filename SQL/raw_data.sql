SELECT 
    e.*,
    SPLIT_PART(geo_location, ',', 1)::DOUBLE PRECISION AS latitude,
    SPLIT_PART(geo_location, ',', 2)::DOUBLE PRECISION AS longitude,
    p.program_name,
    (CAST(eventdate AS DATE) + CAST(eventtime AS TIME)) AS eventdatetime
FROM 
    events e
LEFT JOIN 
    program_mapping p
ON 
    e.program_id = p.program_id;
