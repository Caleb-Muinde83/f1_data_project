{{ config(enabled = (target.name == 'ci')) }}
-- CI only: it relies on the shape of the synthetic qualifying session (laps 1, 4, 7 ... are out-laps with the pit record on them, laps 2, 5, 8 ... are push laps,
-- laps 3, 6, 9 ... are in-laps). On real data qualifying has no such pattern, so the generic guard is assert_out_laps_follow_api_flag.
-- A push lap must not be flagged as an in-lap or out-lap, and must be able to be representative. Fails for every push lap the pit-table rule used to flag.
SELECT session_key, driver_number, lap_number, lap_time_seconds, is_in_lap, is_out_lap, is_representative_lap
FROM {{ ref('int_lap_context') }}
WHERE session_type = 'Qualifying' AND lap_number % 3 = 2 AND lap_time_seconds < 100
  AND (is_in_lap OR is_out_lap OR NOT is_representative_lap)
