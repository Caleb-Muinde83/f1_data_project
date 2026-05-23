WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

stints AS (
    SELECT * FROM {{ ref('stg_stints') }}
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    l.lap_duration AS lap_time_seconds,
    
    -- Bringing in the Tyre Strategy Data
    s.stint_number,
    s.tyre_compound, -- Updated to match the column name coming out of stg_stints
    
    -- Calculate exactly how many laps this set of tyres has done!
    s.tyre_age_at_start + (l.lap_number - s.lap_start) AS tyre_age_this_lap

FROM laps l
-- Range Join
LEFT JOIN stints s
    ON l.session_key = s.session_key
    AND l.driver_number = s.driver_number
    AND l.lap_number BETWEEN s.lap_start AND s.lap_end