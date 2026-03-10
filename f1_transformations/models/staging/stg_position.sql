WITH raw_position AS (
    SELECT * FROM {{ source('openf1_raw', 'position') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS position_date,
        CAST(position AS INTEGER) AS track_position
        
    FROM raw_position
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted