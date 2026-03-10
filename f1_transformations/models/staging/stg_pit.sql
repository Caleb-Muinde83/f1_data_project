WITH raw_pit AS (
    SELECT * FROM {{ source('openf1_raw', 'pit') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(lap_number AS INTEGER) AS lap_number,
        CAST(date AS TIMESTAMP) AS pit_date,
        CAST(pit_duration AS DOUBLE) AS pit_duration
    FROM raw_pit
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted