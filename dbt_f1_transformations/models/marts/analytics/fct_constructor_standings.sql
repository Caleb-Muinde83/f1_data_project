WITH team_standings AS (
    SELECT * FROM {{ ref('stg_championship_teams') }}
)

SELECT
    session_key,
    meeting_key,
    team_name,
    position_start AS season_starting_position,
    position_current AS current_championship_position,
    points_current AS total_points
FROM team_standings
