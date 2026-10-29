{# Per driver per compound: fuel-corrected lap time as a straight line in tyre age
   (intercept = pace on fresh tyres, slope = degradation s/lap, r2 = how well a line explains it).
   Feeds the race strategy simulator. Needs var min_stint_laps_for_degradation clean laps per compound. #}
WITH laps AS (
    SELECT
        session_key, meeting_key, driver_number, tyre_compound,
        CASE WHEN is_representative_lap AND tyre_age_this_lap IS NOT NULL AND COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) IS NOT NULL
             THEN COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) END AS y,
        CASE WHEN is_representative_lap AND tyre_age_this_lap IS NOT NULL AND COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) IS NOT NULL
             THEN {{ f1_cast('tyre_age_this_lap', 'double') }} END AS x
    FROM {{ ref('int_lap_context') }}
    WHERE tyre_compound IS NOT NULL
)
SELECT
    session_key, meeting_key, driver_number, tyre_compound,
    COUNT(y) AS clean_laps,
    {{ f1_regr_intercept('y', 'x') }} AS fresh_tyre_pace_seconds,
    {{ f1_regr_slope('y', 'x') }} AS degradation_seconds_per_lap,
    {{ f1_r2('y', 'x') }} AS r_squared,
    MIN(x) AS min_tyre_age,
    MAX(x) AS max_tyre_age
FROM laps
GROUP BY session_key, meeting_key, driver_number, tyre_compound
HAVING COUNT(y) >= {{ var('min_stint_laps_for_degradation') }}
