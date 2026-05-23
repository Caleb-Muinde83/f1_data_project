WITH pit_stops AS (
    SELECT * FROM {{ ref('stg_pit') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    CAST(pit_duration AS DOUBLE) AS pit_lane_time_seconds
FROM pit_stops
WHERE pit_duration IS NOT NULL