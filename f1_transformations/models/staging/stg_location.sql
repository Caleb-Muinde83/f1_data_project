WITH raw_location AS (
    SELECT * FROM {{ source('openf1_raw', 'location') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS location_date,
        
        CAST(x AS DOUBLE) AS x_coordinate,
        CAST(y AS DOUBLE) AS y_coordinate,
        CAST(z AS DOUBLE) AS z_coordinate
        
    FROM raw_location
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted