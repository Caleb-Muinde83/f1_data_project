WITH raw_champ_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_drivers') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(position_current AS INTEGER) AS championship_position,
        CAST(points_current AS DOUBLE) AS total_points
    FROM raw_champ_drivers
    WHERE meeting_key IS NOT NULL
)

SELECT * FROM renamed_and_casted