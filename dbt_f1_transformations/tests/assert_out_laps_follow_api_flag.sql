-- Where the API flags out-laps at all, a lap is an out-lap exactly when the API says so. The pit-table rule ("the lap after a pit record") flagged every qualifying
-- PUSH lap as an out-lap, because OpenF1 puts the pit record on the out-lap in practice and qualifying.
WITH api AS (
    SELECT session_key, driver_number, MAX(CASE WHEN is_pit_out_lap THEN 1 ELSE 0 END) AS has_api_flags FROM {{ ref('stg_laps') }} GROUP BY session_key, driver_number
)
SELECT c.session_key, c.driver_number, c.lap_number, c.is_out_lap, l.is_pit_out_lap
FROM {{ ref('int_lap_context') }} c
JOIN {{ ref('stg_laps') }} l ON l.session_key = c.session_key AND l.driver_number = c.driver_number AND l.lap_number = c.lap_number
JOIN api ON api.session_key = c.session_key AND api.driver_number = c.driver_number
WHERE api.has_api_flags = 1 AND c.is_out_lap <> COALESCE(l.is_pit_out_lap, FALSE)
