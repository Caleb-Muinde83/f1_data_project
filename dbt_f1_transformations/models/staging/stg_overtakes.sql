WITH raw_overtakes AS (
    SELECT * FROM {{ source('openf1_raw', 'overtakes') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(date AS TIMESTAMP) AS overtake_timestamp,

        CAST(overtaking_driver_number AS INTEGER) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS INTEGER) AS overtaken_driver_number,
        CAST(position AS INTEGER) AS position
    FROM raw_overtakes
)
SELECT * FROM renamed_and_casted
