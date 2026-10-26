-- Within a driver's session, lap N+1 must start after lap N (catches duplicated or mis-numbered laps).
WITH ordered AS (
    SELECT session_key, driver_number, lap_number, date_start,
           LAG(date_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number) AS prev_start
    FROM {{ ref('stg_laps') }}
    WHERE date_start IS NOT NULL
)
SELECT * FROM ordered WHERE prev_start IS NOT NULL AND date_start <= prev_start
