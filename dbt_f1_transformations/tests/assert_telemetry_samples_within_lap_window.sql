-- A telemetry sample may only carry a lap number while that lap is running (cool-down laps and garage time must not be credited to the previous lap).
SELECT t.session_key, t.driver_number, t.lap_number, COUNT(*) AS samples_after_lap_end
FROM {{ ref('fct_telemetry') }} t
JOIN {{ ref('stg_laps') }} l
  ON t.session_key = l.session_key AND t.driver_number = l.driver_number AND t.lap_number = l.lap_number
WHERE l.lap_duration IS NOT NULL
  AND {{ in_scope('t.session_key') }}
  AND {{ f1_epoch_ms('t.telemetry_timestamp') }} - {{ f1_epoch_ms('l.date_start') }}
        > 1000 * (l.lap_duration + {{ var('telemetry_lap_tolerance_seconds', 2) }} + 1)
GROUP BY 1, 2, 3
