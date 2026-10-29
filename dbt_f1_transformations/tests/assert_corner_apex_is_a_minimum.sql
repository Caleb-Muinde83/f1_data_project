{{ config(severity='warn') }}
-- On the REFERENCE lap an apex must be slower than both its entry and exit. Other laps may legitimately differ.
SELECT c.session_key, c.corner_number, p.apex_speed_kph, p.entry_speed_kph, p.exit_speed_kph
FROM {{ ref('dim_session_corners') }} c
JOIN {{ ref('fct_corner_performance') }} p
    ON c.session_key = p.session_key AND c.corner_number = p.corner_number
    AND c.ref_driver_number = p.driver_number AND c.ref_lap_number = p.lap_number
WHERE p.apex_speed_kph > p.entry_speed_kph OR p.apex_speed_kph > p.exit_speed_kph
