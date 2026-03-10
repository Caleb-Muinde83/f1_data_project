WITH raw_stints AS (
    SELECT * FROM {{ source('openf1_raw', 'stints') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(stint_number AS INTEGER) AS stint_number,
        CAST(lap_start AS INTEGER) AS lap_start,
        CAST(lap_end AS INTEGER) AS lap_end,
        compound AS tyre_compound,
        CAST(tyre_age_at_start AS INTEGER) AS tyre_age_at_start
    FROM raw_stints
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted