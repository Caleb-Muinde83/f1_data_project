WITH raw_race_control AS (
    SELECT * FROM {{ source('openf1_raw', 'race_control') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(lap_number AS INTEGER) AS lap_number,
        
        CAST(date AS TIMESTAMP) AS race_control_timestamp,
        category,
        message,
        flag
    FROM raw_race_control
)
SELECT * FROM renamed_and_casted