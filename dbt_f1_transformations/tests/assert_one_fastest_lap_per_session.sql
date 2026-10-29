-- Every session with timed laps must have at least one overall-fastest-lap row.
SELECT session_key, COUNT(CASE WHEN is_overall_fastest_lap THEN 1 END) AS fastest_rows
FROM {{ ref('fct_fastest_laps') }}
GROUP BY session_key
HAVING COUNT(CASE WHEN is_overall_fastest_lap THEN 1 END) = 0
