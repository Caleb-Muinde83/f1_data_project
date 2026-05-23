WITH intervals AS (
    SELECT * FROM {{ ref('stg_intervals') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    gap_to_leader,
    interval AS gap_to_car_ahead,
    
    -- Business Logic: Identify if the driver is in DRS range
    CASE 
        WHEN interval <= 1.000 THEN TRUE 
        ELSE FALSE 
    END AS is_in_drs_range

FROM intervals
-- Filter out the leader, as they don't have a gap to the car ahead!
WHERE interval IS NOT NULL