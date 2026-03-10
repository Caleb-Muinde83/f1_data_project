WITH results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),

sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

final_results AS (
    SELECT
        -- Session Info
        s.meeting_key,
        s.session_key,
        s.country_name,
        s.session_name,
        s.date_start,
        
        -- Driver & Team Info
        r.driver_number,
        d.full_name AS driver_name,
        d.team_name,
        
        -- Result Metrics
        r.final_position,
        r.is_dnf,
        r.is_dns
        
    FROM results r
    LEFT JOIN sessions s 
        ON r.session_key = s.session_key
    LEFT JOIN drivers d 
        ON r.driver_number = d.driver_number 
        -- OpenF1 scopes drivers by session, so joining on both is safest!
        AND r.session_key = d.session_key 
)

SELECT * FROM final_results
-- Sort it so the winner is at the top of each session
ORDER BY date_start DESC, final_position ASC