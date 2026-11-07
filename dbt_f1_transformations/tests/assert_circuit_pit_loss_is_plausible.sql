-- The circuit-level typical pit-lane time feeds the strategy simulator: it must look like a race pit stop (10-120 s), not a garage visit.
SELECT circuit_key, median_green_pit_lane_time_seconds
FROM {{ ref('fct_pit_loss_by_circuit') }}
WHERE median_green_pit_lane_time_seconds < 10 OR median_green_pit_lane_time_seconds > 120
