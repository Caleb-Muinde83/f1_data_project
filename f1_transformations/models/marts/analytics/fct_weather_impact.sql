WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
    WHERE lap_duration IS NOT NULL
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

drivers AS (
    SELECT * FROM {{ ref('dim_drivers') }}
)

SELECT
    l.session_key,
    l.driver_number,
    d.full_name,
    l.lap_number,
    l.lap_duration AS lap_time_seconds,
    
    -- Weather details at the time of the lap
    w.track_temperature,
    w.air_temperature,
    w.rainfall_flag,
    w.wind_speed,
    w.humidity

FROM laps l
-- The magic ASOF JOIN: Match the session, then find the closest weather record just BEFORE the lap started
ASOF LEFT JOIN weather w
    ON l.session_key = w.session_key 
    AND l.date_start >= w.date_recorded 
LEFT JOIN drivers d
    ON l.driver_number = d.driver_number
ORDER BY l.session_key DESC, l.date_start ASC