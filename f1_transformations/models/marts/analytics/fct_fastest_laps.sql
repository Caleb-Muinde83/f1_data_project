WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
    -- We must filter out NULLs (like pit-in laps or incomplete laps)
    WHERE lap_duration IS NOT NULL 
),

driver_personal_bests AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        lap_duration,
        -- Rank laps for EACH driver in EACH session
        RANK() OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY lap_duration ASC
        ) as personal_lap_rank
    FROM laps
),

session_overall_fastest AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        lap_duration,
        -- Now, rank the personal bests against EVERYONE in the session
        RANK() OVER (
            PARTITION BY session_key 
            ORDER BY lap_duration ASC
        ) as overall_session_rank
    FROM driver_personal_bests
    -- Only look at their absolute best lap
    WHERE personal_lap_rank = 1
),

drivers AS (
    SELECT * FROM {{ ref('dim_drivers') }}
)

SELECT 
    s.session_key,
    s.driver_number,
    d.full_name,
    d.team_name,
    s.lap_number AS fastest_lap_number,
    s.lap_duration AS fastest_lap_time_seconds,
    s.overall_session_rank,
    -- Flag the purple lap! 🟪
    CASE WHEN s.overall_session_rank = 1 THEN TRUE ELSE FALSE END AS is_overall_fastest_lap
FROM session_overall_fastest s
LEFT JOIN drivers d
    ON s.driver_number = d.driver_number
ORDER BY s.session_key DESC, s.overall_session_rank ASC