WITH raw_car_data AS (
    SELECT * FROM {{ source('openf1_raw', 'car_data') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS telemetry_date,
        
        CAST(rpm AS INTEGER) AS rpm,
        CAST(speed AS INTEGER) AS speed,
        CAST(n_gear AS INTEGER) AS n_gear,
        CAST(throttle AS INTEGER) AS throttle,
        CAST(brake AS INTEGER) AS brake,
        CAST(drs AS INTEGER) AS drs
        
    FROM raw_car_data
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted