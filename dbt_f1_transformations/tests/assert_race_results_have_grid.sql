{{ config(severity='warn') }}
-- Races where NO driver has a grid position: starting_grid rows were not extracted or are keyed to a session we cannot map.
-- Without them positions gained/lost and the grid-vs-finish baseline are empty.
SELECT session_key
FROM {{ ref('fct_race_results') }}
WHERE session_type IN ('Race', 'Sprint')
GROUP BY session_key
HAVING COUNT(grid_position) = 0
