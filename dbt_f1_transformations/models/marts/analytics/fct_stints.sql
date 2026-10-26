{# One row per driver stint with pace and DEGRADATION. degradation = slope of (fuel-corrected) lap time against
   tyre age over representative laps; positive = the tyre is losing time. Needs
   var min_stint_laps_for_degradation representative laps, otherwise NULL (too little evidence). #}
SELECT
    session_key, meeting_key, driver_number, stint_number, tyre_compound,
    MIN(lap_number) AS first_lap,
    MAX(lap_number) AS last_lap,
    COUNT(*) AS stint_length_laps,
    MIN(tyre_age_this_lap) AS tyre_age_at_start,
    MAX(tyre_age_this_lap) AS tyre_age_at_end,
    COUNT(*) FILTER (WHERE is_representative_lap) AS representative_laps,
    AVG(lap_time_seconds) FILTER (WHERE is_representative_lap) AS avg_representative_lap_seconds,
    MIN(lap_time_seconds) FILTER (WHERE is_representative_lap) AS best_representative_lap_seconds,
    CASE
        WHEN COUNT(*) FILTER (WHERE is_representative_lap) >= {{ var('min_stint_laps_for_degradation') }}
        THEN REGR_SLOPE(COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds), tyre_age_this_lap)
             FILTER (WHERE is_representative_lap)
    END AS degradation_seconds_per_lap
FROM {{ ref('int_lap_context') }}
WHERE stint_number IS NOT NULL
GROUP BY session_key, meeting_key, driver_number, stint_number, tyre_compound
