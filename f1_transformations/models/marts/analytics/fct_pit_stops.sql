WITH pit_stops AS (
    SELECT * FROM {{ ref('stg_pit') }}
    -- Filter out any null durations (in case of API glitches or incomplete data)
    WHERE pit_duration IS NOT NULL
),

drivers AS (
    SELECT * FROM {{ ref('dim_drivers') }}
),

pit_analytics AS (
    SELECT
        p.session_key,
        p.driver_number,
        d.full_name,
        d.team_name,
        p.lap_number,
        p.pit_duration AS pit_lane_time_seconds,
        
        -- Flag unusually slow stops (e.g., over 30 seconds total pit lane time)
        CASE WHEN p.pit_duration > 30.0 THEN TRUE ELSE FALSE END AS is_slow_stop,
        
        -- Rank the pit stops within the same session
        RANK() OVER (
            PARTITION BY p.session_key 
            ORDER BY p.pit_duration ASC
        ) AS session_pit_rank,

        d.team_color_hex
    FROM pit_stops p
    LEFT JOIN drivers d
        ON p.driver_number = d.driver_number
)

SELECT * FROM pit_analytics
ORDER BY session_key DESC, pit_lane_time_seconds ASC