{# One row per driver per session: the whole tyre strategy on one line, e.g. MEDIUM>HARD. #}
SELECT
    session_key, meeting_key, driver_number,
    STRING_AGG(tyre_compound, '>' ORDER BY stint_number) AS strategy,
    COUNT(*) AS stint_count,
    COUNT(*) - 1 AS pit_stop_count,
    SUM(stint_length_laps) AS total_laps,
    LIST(stint_length_laps ORDER BY stint_number) AS stint_lengths,
    COUNT(DISTINCT tyre_compound) FILTER (WHERE tyre_compound IN ('SOFT', 'MEDIUM', 'HARD')) >= 2 AS used_two_dry_compounds
FROM {{ ref('fct_stints') }}
GROUP BY session_key, meeting_key, driver_number
