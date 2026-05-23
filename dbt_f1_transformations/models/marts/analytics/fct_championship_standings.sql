WITH driver_standings AS (
    SELECT * FROM {{ ref('stg_championship_drivers') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    position_start AS season_starting_position,
    position_current AS current_championship_position,
    points_current AS total_points
FROM driver_standings