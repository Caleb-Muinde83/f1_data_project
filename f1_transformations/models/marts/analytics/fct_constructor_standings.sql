WITH team_standings AS (
    SELECT * FROM {{ ref('stg_championship_teams') }}
),

final_standings AS (
    SELECT
        team_name,
        total_points,
        championship_position,
        -- Calculate the gap to the leading team
        MAX(total_points) OVER () - total_points AS gap_to_leader
    FROM team_standings
)

SELECT * FROM final_standings
ORDER BY championship_position ASC