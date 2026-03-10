WITH driver_standings AS (
    SELECT * FROM {{ ref('stg_championship_drivers') }}
),

drivers AS (
    SELECT * FROM {{ ref('dim_drivers') }}
),

final_standings AS (
    SELECT
        d.full_name,
        d.team_name,
        -- Using your staging aliases: total_points and championship_position
        s.total_points,
        s.championship_position,
        
        -- Calculate the gap to the leader using the correct column names
        FIRST_VALUE(s.total_points) OVER (ORDER BY s.championship_position ASC) - s.total_points AS gap_to_leader,
        
        d.team_color_hex,
        d.headshot_url
    FROM driver_standings s
    LEFT JOIN drivers d 
        ON s.driver_number = d.driver_number
)

SELECT * FROM final_standings
-- Order by the official standing position
ORDER BY championship_position ASC