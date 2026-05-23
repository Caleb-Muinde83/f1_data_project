WITH raw_pit AS (
    SELECT * FROM {{ source('openf1_raw', 'pit') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(lap_number AS INTEGER) AS lap_number,
        pit_duration
    FROM raw_pit
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted