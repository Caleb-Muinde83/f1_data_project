WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

ranked_laps AS (
    SELECT
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        lap_duration AS fastest_lap_time_seconds,
        -- Row number orders laps from quickest to slowest per driver, per session
        ROW_NUMBER() OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY lap_duration ASC
        ) AS lap_rank
    FROM laps
    WHERE lap_duration IS NOT NULL
)

-- Grab only the #1 fastest lap for every driver in each session
SELECT
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    fastest_lap_time_seconds
FROM ranked_laps
WHERE lap_rank = 1