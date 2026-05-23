WITH raw_weather AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/weather_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        CAST(date AS TIMESTAMP) AS weather_timestamp,
        CAST(air_temperature AS DOUBLE) AS air_temperature,
        CAST(track_temperature AS DOUBLE) AS track_temperature,
        CAST(humidity AS DOUBLE) AS humidity,
        CAST(rainfall AS INTEGER) AS rainfall_flag,
        CAST(wind_speed AS DOUBLE) AS wind_speed,
        CAST(wind_direction AS INTEGER) AS wind_direction
    FROM raw_weather
)
SELECT * FROM renamed_and_casted