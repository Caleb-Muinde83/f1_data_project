WITH raw_position AS (
    SELECT * FROM {{ source('openf1_raw', 'position') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(position AS {{ f1_type('int') }}) AS position,
        CAST(date AS {{ f1_type('timestamp') }}) AS position_timestamp
    FROM raw_position
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted