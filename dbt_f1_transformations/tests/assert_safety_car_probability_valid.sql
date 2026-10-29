-- Probabilities must lie in [0, 1] and never fall as the horizon lengthens.
WITH p AS (
    SELECT circuit_key, track_status, laps_remaining, p_at_least_one_start,
           LAG(p_at_least_one_start) OVER (PARTITION BY circuit_key, track_status ORDER BY laps_remaining) AS prev_p
    FROM {{ ref('fct_safety_car_hazard') }}
)
SELECT * FROM p WHERE p_at_least_one_start < 0 OR p_at_least_one_start > 1 OR p_at_least_one_start < prev_p
