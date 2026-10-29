WITH raw_championship_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_drivers') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        position_start,
        position_current,
        CAST(points_start AS {{ f1_type('double') }}) AS points_start,
        CAST(points_current AS {{ f1_type('double') }}) AS points_current
    FROM raw_championship_drivers
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted