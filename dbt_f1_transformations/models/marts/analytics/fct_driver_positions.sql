{{ config(
    materialized = 'table'
) }}

WITH positions_data AS (
    SELECT * FROM {{ ref('stg_position') }}
),

session_context AS (
    SELECT * FROM {{ ref('dim_sessions') }}
)

SELECT
    p.session_key,
    p.meeting_key,
    p.driver_number,
    
    -- UPDATE THIS LINE: Use the actual column name from your staging model
    p.position_timestamp, 
    
    p.position AS live_position,
    
    -- Update the ORDER BY clauses here too
    LAG(p.position) OVER (
        PARTITION BY p.session_key, p.driver_number 
        ORDER BY p.position_timestamp
    ) AS previous_position,
    
    CASE 
        WHEN LAG(p.position) OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.position_timestamp) > p.position THEN TRUE
        ELSE FALSE
    END AS gained_position,
    
    CASE 
        WHEN LAG(p.position) OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.position_timestamp) < p.position THEN TRUE
        ELSE FALSE
    END AS lost_position

FROM positions_data p