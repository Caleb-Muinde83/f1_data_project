WITH raw_position AS (
    SELECT * FROM {{ source('openf1_raw', 'position') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(position AS INTEGER) AS position,
        CAST(date AS TIMESTAMP) AS position_timestamp
    FROM raw_position
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted