-- Road-car-impossible telemetry (negative or > 400 km/h) points to a parsing or unit problem.
SELECT session_key, driver_number, telemetry_timestamp, speed
FROM {{ ref('fct_telemetry') }}
WHERE speed < 0 OR speed > 400
LIMIT 100
