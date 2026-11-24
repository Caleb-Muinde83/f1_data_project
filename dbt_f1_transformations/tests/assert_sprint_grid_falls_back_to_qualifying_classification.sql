{{ config(enabled = (target.name == 'ci')) }}
-- CI only: the synthetic Sprint 9006 (meeting 1003) has NO starting_grid rows at all, as the real 2025 Brazilian Sprint does, so its grid must be the classification of its
-- Sprint Qualifying (9005): drivers 63, 44, 1, 11 finished 1st to 4th there. Fails for any other grid position or source.
SELECT driver_number, grid_position, grid_source
FROM {{ ref('fct_race_results') }}
WHERE session_key = 9006
  AND (grid_source IS DISTINCT FROM 'qualifying_classification'
       OR grid_position <> CASE driver_number WHEN 63 THEN 1 WHEN 44 THEN 2 WHEN 1 THEN 3 WHEN 11 THEN 4 END)
