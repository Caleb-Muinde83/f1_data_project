WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
    WHERE lap_duration IS NOT NULL
),

-- Aggregate race control messages by session and lap
-- so we don't duplicate lap records if there were multiple yellow flags in one lap
race_control_events AS (
    SELECT 
        session_key,
        lap_number,
        -- Combine all categories into one string (e.g., "SafetyCar, Flag")
        STRING_AGG(DISTINCT category, ', ') AS incident_categories,
        -- Get the most severe flag shown on that lap
        MAX(flag) AS primary_flag
    FROM {{ ref('stg_race_control') }}
    WHERE lap_number IS NOT NULL
    GROUP BY 
        session_key, 
        lap_number
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    l.lap_duration AS lap_time_seconds,
    
    -- Business Logic: Flag unrepresentative laps
    CASE 
        WHEN r.incident_categories IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_affected_by_incident,
    
    r.incident_categories,
    r.primary_flag

FROM laps l
LEFT JOIN race_control_events r
    ON l.session_key = r.session_key
    AND l.lap_number = r.lap_number