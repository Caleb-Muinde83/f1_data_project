WITH raw_session_result AS (
    SELECT * FROM {{ source('openf1_raw', 'session_result') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(position AS INTEGER) AS position,
        duration, -- Added based on candidate bindings
        
        dns,
        dsq
        
    FROM raw_session_result
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted