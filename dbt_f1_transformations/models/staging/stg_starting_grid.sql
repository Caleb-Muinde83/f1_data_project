WITH raw_starting_grid AS (
    SELECT * FROM {{ source('openf1_raw', 'starting_grid') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        -- Changed from grid_position to position based on likely API schema
        CAST(position AS INTEGER) AS grid_position
        
    FROM raw_starting_grid
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted