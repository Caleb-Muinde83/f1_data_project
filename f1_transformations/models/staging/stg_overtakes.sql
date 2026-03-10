WITH raw_overtakes AS (
    SELECT * FROM {{ source('openf1_raw', 'overtakes') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(overtaking_driver_number AS INTEGER) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS INTEGER) AS overtaken_driver_number,
        CAST(date AS TIMESTAMP) AS overtake_date,
        CAST(position AS INTEGER) AS position_gained
    FROM raw_overtakes
    -- DEFENSIVE FILTER: Drop rows that are missing critical ID info
    WHERE session_key IS NOT NULL
      AND overtaking_driver_number IS NOT NULL
      AND overtaken_driver_number IS NOT NULL
)

SELECT * FROM renamed_and_casted