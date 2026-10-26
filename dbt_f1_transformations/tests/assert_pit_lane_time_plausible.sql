{{ config(severity='warn') }}
-- Pit lane time outside 10-120 s is usually a red-flag stop or bad data: review, do not trust blindly.
SELECT session_key, driver_number, lap_number, pit_lane_time_seconds
FROM {{ ref('fct_pit_stops') }}
WHERE pit_lane_time_seconds < 10 OR pit_lane_time_seconds > 120
