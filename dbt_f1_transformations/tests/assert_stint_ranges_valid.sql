-- A stint cannot end before it starts, and tyre age cannot be negative.
SELECT session_key, driver_number, stint_number, lap_start, lap_end, tyre_age_at_start
FROM {{ ref('stg_stints') }}
WHERE lap_end < lap_start OR tyre_age_at_start < 0
