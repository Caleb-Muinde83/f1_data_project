-- A throttle above 100 is "no data" and must never reach the distance trace as a value.
SELECT session_key, driver_number, lap_number, distance_bin, avg_throttle
FROM {{ ref('fct_telemetry_distance_trace') }}
WHERE avg_throttle > 100
