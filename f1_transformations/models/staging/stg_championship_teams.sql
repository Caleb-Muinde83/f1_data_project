WITH raw_champ_teams AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_teams') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        -- The API gives us team_name instead of team_id!
        team_name AS team_name, 
        
        CAST(position_current AS INTEGER) AS championship_position,
        CAST(points_current AS DOUBLE) AS total_points
        
    FROM raw_champ_teams
    WHERE meeting_key IS NOT NULL
)

SELECT * FROM renamed_and_casted