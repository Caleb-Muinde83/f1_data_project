WITH session_results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),

starting_grid AS (
    SELECT * FROM {{ ref('stg_starting_grid') }}
),

race_results_calculated AS (
    SELECT
        r.session_key,
        r.meeting_key,
        r.driver_number,
        g.grid_position,
        r.position AS final_position,
        
        -- If both positions exist, calculate the net positions gained (+) or lost (-)
        CASE 
            WHEN g.grid_position IS NOT NULL AND r.position IS NOT NULL 
            THEN (g.grid_position - r.position)
            ELSE NULL 
        END AS positions_gained_or_lost,
        
        r.duration AS race_duration,
        r.dns AS did_not_start,
        r.dsq AS disqualified
        
    FROM session_results r
    -- We use a LEFT JOIN because a driver might have a race result 
    -- but missed qualifying (starting from the pit lane / no grid position)
    LEFT JOIN starting_grid g
        ON r.session_key = g.session_key
        AND r.driver_number = g.driver_number
)

SELECT * FROM race_results_calculated