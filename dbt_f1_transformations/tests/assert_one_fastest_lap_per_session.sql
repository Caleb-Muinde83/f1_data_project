-- Every session with timed laps must have at least one overall-fastest-lap row.
SELECT session_key, COUNT(*) FILTER (WHERE is_overall_fastest_lap) AS fastest_rows
FROM {{ ref('fct_fastest_laps') }}
GROUP BY session_key
HAVING COUNT(*) FILTER (WHERE is_overall_fastest_lap) = 0
