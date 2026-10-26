{{ config(severity='warn') }}
-- Reported track positions should be between 1 and 24.
SELECT session_key, driver_number, lap_number, position_end
FROM {{ ref('int_lap_context') }}
WHERE position_end IS NOT NULL AND (position_end < 1 OR position_end > 24)
