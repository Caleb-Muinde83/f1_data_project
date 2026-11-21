-- A stop counted as a green-flag stop must have a green in-lap AND out-lap: otherwise its time lost (and the circuit pit-loss medians built from it)
-- is inflated by the neutralised lap. Fails for any stop that is not flagged as neutralised although one of its two laps was.
SELECT session_key, driver_number, stop_number, lap_number, in_lap_track_status, out_lap_track_status
FROM {{ ref('fct_pit_stops') }}
WHERE NOT is_under_neutralisation
  AND (in_lap_track_status IN ('SC', 'VSC', 'RED') OR out_lap_track_status IN ('SC', 'VSC', 'RED'))
