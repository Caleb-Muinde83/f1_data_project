WITH raw_overtakes AS (
    SELECT * FROM {{ source('openf1_raw', 'overtakes') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        CAST(overtaking_driver_number AS INTEGER) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS INTEGER) AS overtaken_driver_number,
        
        -- Removed lap_number and added position based on the API's actual schema
        CAST(position AS INTEGER) AS position
        
    FROM raw_overtakes
)
SELECT * FROM renamed_and_casted