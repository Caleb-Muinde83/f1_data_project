{{ config(enabled = (target.name == 'ci')) }}
-- CI only: the synthetic data freezes the first 3 s of lap 5 of driver 44 at 168 km/h, gear 4, throttle 104 (no data), as the real qualifying lap 19 of VER was frozen.
-- Those bins must come out of the distance trace with NULL channels. Fails if any speed or throttle survives in the first 2 s of that lap.
SELECT session_key, driver_number, lap_number, distance_bin, elapsed_seconds, avg_speed_kph, avg_throttle
FROM {{ ref('fct_telemetry_distance_trace') }}
WHERE driver_number = 44 AND lap_number = 5 AND elapsed_seconds < 2.0
  AND {{ in_scope('session_key') }}
  AND (avg_speed_kph IS NOT NULL OR avg_throttle IS NOT NULL)
