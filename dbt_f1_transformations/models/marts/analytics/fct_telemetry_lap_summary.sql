{# Per driver per lap telemetry KPIs: top/min speed, throttle/brake application, gear usage, DRS and braking zones. #}
WITH samples AS (
    SELECT
        *,
        LAG(brake) OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp) AS prev_brake
    FROM {{ ref('fct_telemetry') }}
    WHERE lap_number IS NOT NULL
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    COUNT(*) AS telemetry_samples,
    MAX(speed) AS top_speed_kph,
    MIN(speed) AS min_speed_kph,
    AVG(speed) AS avg_speed_kph,
    AVG((throttle >= 99)::INTEGER) AS full_throttle_fraction,
    AVG((brake > 0)::INTEGER) AS braking_fraction,
    -- a braking zone starts when brake goes from released (0) to applied (>0)
    COUNT(*) FILTER (WHERE brake > 0 AND COALESCE(prev_brake, 0) = 0) AS braking_zone_count,
    AVG((drs IN (10, 12, 14))::INTEGER) AS drs_open_fraction,
    MAX(gear) AS max_gear,
    AVG((gear <= 3)::INTEGER) AS low_gear_fraction,
    AVG((gear >= 7)::INTEGER) AS high_gear_fraction
FROM samples
GROUP BY session_key, meeting_key, driver_number, lap_number
