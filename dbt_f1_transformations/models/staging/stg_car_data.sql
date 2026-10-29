WITH raw_car_data AS (
    SELECT * FROM {{ source('openf1_raw', 'car_data') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(date AS {{ f1_type('timestamp') }}) AS telemetry_timestamp,
        CAST(rpm AS {{ f1_type('int') }}) AS rpm,
        CAST(speed AS {{ f1_type('int') }}) AS speed,
        CAST(n_gear AS {{ f1_type('int') }}) AS gear,
        CAST(throttle AS {{ f1_type('int') }}) AS throttle,
        CAST(brake AS {{ f1_type('int') }}) AS brake,
        CAST(drs AS {{ f1_type('int') }}) AS drs
    FROM raw_car_data
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted