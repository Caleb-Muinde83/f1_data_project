WITH raw_race_control AS (
    SELECT * FROM {{ source('openf1_raw', 'race_control') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        -- TRY_CAST: an all-empty Parquet column can be typed as text, which would crash a plain CAST
        TRY_CAST(lap_number AS INTEGER) AS lap_number,
        TRY_CAST(driver_number AS INTEGER) AS driver_number,

        CAST(date AS TIMESTAMP) AS race_control_timestamp,
        category,
        message,
        flag,
        scope,
        TRY_CAST(sector AS INTEGER) AS sector
    FROM raw_race_control
)
SELECT * FROM renamed_and_casted
