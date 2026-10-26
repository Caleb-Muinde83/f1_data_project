-- Every recorded lap time must be a positive number of seconds.
SELECT session_key, driver_number, lap_number, lap_time_seconds
FROM {{ ref('fct_lap_performance') }}
WHERE lap_time_seconds IS NOT NULL AND lap_time_seconds <= 0
