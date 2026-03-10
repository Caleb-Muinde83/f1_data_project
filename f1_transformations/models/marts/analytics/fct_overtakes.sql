WITH overtakes AS (
    SELECT * FROM {{ ref('stg_overtakes') }}
),

sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

final_overtakes AS (
    SELECT
        -- Session Context
        s.meeting_key,
        s.session_key,
        s.country_name,
        s.session_name,
        
        -- Overtake Timing & Result
        o.overtake_date,
        o.position_gained,
        
        -- The Attacker (Overtaking Driver)
        o.overtaking_driver_number,
        attacker.full_name AS overtaking_driver_name,
        attacker.team_name AS overtaking_team_name,
        
        -- The Defender (Overtaken Driver)
        o.overtaken_driver_number,
        defender.full_name AS overtaken_driver_name,
        defender.team_name AS overtaken_team_name
        
-- models/marts/fct_overtakes.sql

-- ... keep the WITH blocks the same ...

    FROM overtakes o
    
    -- Change this from LEFT JOIN to INNER JOIN
    INNER JOIN sessions s
        ON o.session_key = s.session_key
        
    LEFT JOIN drivers attacker
        ON o.overtaking_driver_number = attacker.driver_number
        AND o.session_key = attacker.session_key
        
    LEFT JOIN drivers defender
        ON o.overtaken_driver_number = defender.driver_number
        AND o.session_key = defender.session_key
)

SELECT * FROM final_overtakes
-- Sort chronologically so we can see the story of the race unfold
ORDER BY overtake_date ASC