{# Season points after every Race/Sprint, computed from fct_race_results (not from the beta standings endpoint),
   so it is complete for every session that has results. #}
WITH scored AS (
    SELECT r.session_key, r.driver_number, r.year, r.session_type, r.total_points, s.session_start_time
    FROM {{ ref('fct_race_results') }} r
    JOIN {{ ref('dim_sessions') }} s ON r.session_key = s.session_key
    WHERE r.session_type IN ('Race', 'Sprint')
),
cum AS (
    SELECT
        *,
        SUM(total_points) OVER (PARTITION BY year, driver_number ORDER BY session_start_time, session_key) AS cumulative_points
    FROM scored
)
SELECT
    session_key, year, driver_number, session_type, session_start_time,
    total_points AS points_scored_in_session,
    cumulative_points,
    MAX(cumulative_points) OVER (PARTITION BY year, session_key) - cumulative_points AS gap_to_points_leader,
    RANK() OVER (PARTITION BY year, session_key ORDER BY cumulative_points DESC) AS championship_position_after_session
FROM cum
