-- Duplicated result rows would inflate points: position points in one Race cannot exceed 101 (25+18+15+12+10+8+6+4+2+1)
-- and in one Sprint cannot exceed 36.
SELECT session_key, session_type, SUM(position_points) AS total_position_points
FROM {{ ref('fct_race_results') }}
GROUP BY session_key, session_type
HAVING (session_type = 'Race' AND SUM(position_points) > 101)
    OR (session_type = 'Sprint' AND SUM(position_points) > 36)
