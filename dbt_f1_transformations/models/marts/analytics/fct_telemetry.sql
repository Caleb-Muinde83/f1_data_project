{{ config(
    materialized = 'view'
) }}

WITH car_data AS (
    SELECT * FROM {{ ref('stg_car_data') }}
),

location_data AS (
    SELECT * FROM {{ ref('stg_location') }}
)

SELECT
    c.session_key,
    c.meeting_key,
    c.driver_number,
    c.telemetry_timestamp,
    
    -- Performance Metrics (Car Data)
    c.rpm,
    c.speed,
    c.gear,
    c.throttle,
    c.brake,
    c.drs,
    
    -- Track Position (Location Data)
    l.x_coordinate AS x,
    l.y_coordinate AS y,
    l.z_coordinate AS z
    
FROM car_data c
LEFT JOIN location_data l
    ON c.session_key = l.session_key
    AND c.driver_number = l.driver_number
    AND c.telemetry_timestamp = l.location_timestamp