{{ config(severity='warn') }}
-- Every result row should resolve to a driver in dim_driver_session (team attribution).
SELECT r.session_key, r.driver_number
FROM {{ ref('fct_race_results') }} r
LEFT JOIN {{ ref('dim_driver_session') }} d ON r.session_key = d.session_key AND r.driver_number = d.driver_number
WHERE d.driver_number IS NULL
