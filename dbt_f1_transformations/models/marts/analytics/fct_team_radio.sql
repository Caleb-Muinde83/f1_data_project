{{ config(
    materialized = 'table'
) }}

WITH radio_data AS (
    SELECT * FROM {{ ref('stg_team_radio') }}
),

drivers AS (
    SELECT DISTINCT driver_number, team_name, full_name 
    FROM {{ ref('dim_drivers') }}
)

SELECT
    r.session_key,
    r.meeting_key,
    r.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    
    -- UPDATE THIS LINE: Use the actual column name from your staging model
    r.radio_timestamp,
    
    r.recording_url

FROM radio_data r
LEFT JOIN drivers d
    ON r.driver_number = d.driver_number