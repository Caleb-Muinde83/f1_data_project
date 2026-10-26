{# Per driver per lap: how long was the car in DRS range / dirty air, and the gap evolution. Time-weighted. #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    COUNT(*) AS interval_samples,
    SUM(sample_duration_seconds) AS covered_seconds,
    SUM(sample_duration_seconds) FILTER (WHERE is_in_drs_range) AS seconds_in_drs_range,
    SUM(sample_duration_seconds) FILTER (WHERE is_in_dirty_air) AS seconds_in_dirty_air,
    SUM(sample_duration_seconds) FILTER (WHERE is_in_dirty_air) / NULLIF(SUM(sample_duration_seconds), 0) AS pct_lap_in_dirty_air,
    AVG(gap_to_car_ahead_seconds) AS avg_gap_to_car_ahead_seconds,
    MIN(gap_to_car_ahead_seconds) AS min_gap_to_car_ahead_seconds,
    ARG_MAX(gap_to_leader_seconds, interval_timestamp) AS gap_to_leader_last_sample_seconds
FROM {{ ref('fct_race_intervals') }}
WHERE lap_number IS NOT NULL
GROUP BY session_key, meeting_key, driver_number, lap_number
