WITH raw_weather AS (
    SELECT * FROM {{ source('openf1_raw', 'weather') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(date AS TIMESTAMP) AS date_recorded,
        CAST(air_temperature AS DOUBLE) AS air_temperature,
        CAST(track_temperature AS DOUBLE) AS track_temperature,
        CAST(humidity AS DOUBLE) AS humidity,
        CAST(pressure AS DOUBLE) AS pressure,
        CAST(wind_direction AS INTEGER) AS wind_direction,
        CAST(wind_speed AS DOUBLE) AS wind_speed,
        CAST(rainfall AS INTEGER) AS rainfall_flag
    FROM raw_weather
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted