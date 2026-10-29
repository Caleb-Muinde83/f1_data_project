WITH raw_starting_grid AS (
    SELECT * FROM {{ source('openf1_raw', 'starting_grid') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        -- Changed from grid_position to position based on likely API schema
        CAST(position AS {{ f1_type('int') }}) AS grid_position
        
    FROM raw_starting_grid
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted