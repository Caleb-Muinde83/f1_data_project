WITH raw_race_control AS (
    SELECT * FROM {{ source('openf1_raw', 'race_control') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(lap_number AS INTEGER) AS lap_number,
        CAST(date AS TIMESTAMP) AS event_date,
        category,
        message,
        flag,
        scope,
        CAST(sector AS INTEGER) AS sector
    FROM raw_race_control
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted