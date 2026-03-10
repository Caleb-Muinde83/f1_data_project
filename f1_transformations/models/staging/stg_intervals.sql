WITH raw_intervals AS (
    SELECT * FROM {{ source('openf1_raw', 'intervals') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS interval_date,
        
        CAST(gap_to_leader AS DOUBLE) AS gap_to_leader,
        CAST(interval AS DOUBLE) AS interval_to_car_ahead
        
    FROM raw_intervals
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted