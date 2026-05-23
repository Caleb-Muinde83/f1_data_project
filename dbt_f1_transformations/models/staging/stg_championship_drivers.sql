WITH raw_championship_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_drivers') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        position_start,
        position_current,
        CAST(points_start AS DOUBLE) AS points_start,
        CAST(points_current AS DOUBLE) AS points_current
    FROM raw_championship_drivers
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted