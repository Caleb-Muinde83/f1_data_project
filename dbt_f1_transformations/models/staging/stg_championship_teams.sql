WITH raw_championship_teams AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_teams') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        
        team_name,
        position_start,
        position_current,
        CAST(points_start AS {{ f1_type('double') }}) AS points_start,
        CAST(points_current AS {{ f1_type('double') }}) AS points_current
    FROM raw_championship_teams
)
SELECT * FROM renamed_and_casted