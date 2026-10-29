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
    COUNT(CASE WHEN is_representative_lap THEN 1 END) AS representative_laps,
    AVG(CASE WHEN is_representative_lap THEN lap_time_seconds END) AS avg_representative_lap_seconds,
    MIN(CASE WHEN is_representative_lap THEN lap_time_seconds END) AS best_representative_lap_seconds,
    CASE
        WHEN COUNT(CASE WHEN is_representative_lap THEN 1 END) >= {{ var('min_stint_laps_for_degradation') }}
        THEN {{ f1_regr_slope(
                'CASE WHEN is_representative_lap THEN COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) END',
                "CASE WHEN is_representative_lap THEN " ~ f1_cast('tyre_age_this_lap', 'double') ~ " END") }}
    END AS degradation_seconds_per_lap
FROM {{ ref('int_lap_context') }}
WHERE stint_number IS NOT NULL
GROUP BY session_key, meeting_key, driver_number, stint_number, tyre_compound
