-- A session's grid comes either from the starting_grid feed or, when the feed has nothing for the session at all, from the classification of the session that set it.
-- Mixing the two inside one session would put some drivers on a real grid position and some on a qualifying position. Fails for any such session.
SELECT session_key
FROM {{ ref('fct_race_results') }}
GROUP BY session_key
HAVING SUM(CASE WHEN grid_source = 'qualifying_classification' THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN grid_source IN ('starting_grid', 'starting_grid_via_qualifying') THEN 1 ELSE 0 END) > 0
