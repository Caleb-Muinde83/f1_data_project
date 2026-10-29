WITH raw_location AS (
    SELECT * FROM {{ source('openf1_raw', 'location') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(date AS {{ f1_type('timestamp') }}) AS location_timestamp,
        CAST(x AS {{ f1_type('double') }}) AS x_coordinate,
        CAST(y AS {{ f1_type('double') }}) AS y_coordinate,
        CAST(z AS {{ f1_type('double') }}) AS z_coordinate
    FROM raw_location
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted