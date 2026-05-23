WITH session_weather AS (
    SELECT
        session_key,
        AVG(air_temperature) AS avg_air_temperature,
        AVG(track_temperature) AS avg_track_temperature,
        MAX(rainfall_flag) AS rained_during_session
    FROM {{ ref('stg_weather') }}
    GROUP BY session_key
),

laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    -- Aliased to match your schema.yml testing expectations perfectly
    l.lap_duration AS lap_time_seconds,
    w.avg_air_temperature,
    w.avg_track_temperature,
    w.rained_during_session
FROM laps l
LEFT JOIN session_weather w
    ON l.session_key = w.session_key
WHERE l.lap_duration IS NOT NULL