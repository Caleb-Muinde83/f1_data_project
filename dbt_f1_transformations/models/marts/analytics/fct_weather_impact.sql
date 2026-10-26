{# Weather at the START of each lap (ASOF join to the latest reading at or before lap start), plus the
   session-level averages kept for backwards compatibility. #}
SELECT
    l.session_key, l.meeting_key, l.driver_number, l.lap_number,
    l.lap_time_seconds,
    l.air_temperature, l.track_temperature, l.humidity, l.wind_speed,
    l.is_raining_at_lap_start,
    l.track_temperature - FIRST_VALUE(l.track_temperature) OVER (PARTITION BY l.session_key, l.driver_number ORDER BY l.lap_number)
        AS track_temperature_change_since_first_lap,
    AVG(l.air_temperature) OVER (PARTITION BY l.session_key) AS avg_air_temperature,
    AVG(l.track_temperature) OVER (PARTITION BY l.session_key) AS avg_track_temperature,
    BOOL_OR(l.is_raining_at_lap_start) OVER (PARTITION BY l.session_key) AS rained_during_session,
    l.track_status, l.is_representative_lap, l.delta_to_driver_median_seconds
FROM {{ ref('int_lap_context') }} l
WHERE l.lap_time_seconds IS NOT NULL
