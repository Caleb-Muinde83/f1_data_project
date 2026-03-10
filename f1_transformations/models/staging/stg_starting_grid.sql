WITH raw_starting_grid AS (
    SELECT * FROM {{ source('openf1_raw', 'starting_grid') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(position AS INTEGER) AS starting_position
    FROM raw_starting_grid
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted