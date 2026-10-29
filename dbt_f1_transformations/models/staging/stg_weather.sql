WITH raw_weather AS (
    SELECT * FROM {{ source('openf1_raw', 'weather') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,

        CAST(date AS {{ f1_type('timestamp') }}) AS weather_timestamp,
        CAST(air_temperature AS {{ f1_type('double') }}) AS air_temperature,
        CAST(track_temperature AS {{ f1_type('double') }}) AS track_temperature,
        CAST(humidity AS {{ f1_type('double') }}) AS humidity,
        {{ f1_try_cast_fn() }}(pressure AS {{ f1_type('double') }}) AS pressure,
        CAST(rainfall AS {{ f1_type('int') }}) AS rainfall_flag,
        CAST(wind_speed AS {{ f1_type('double') }}) AS wind_speed,
        CAST(wind_direction AS {{ f1_type('int') }}) AS wind_direction
    FROM raw_weather
)
SELECT * FROM renamed_and_casted
