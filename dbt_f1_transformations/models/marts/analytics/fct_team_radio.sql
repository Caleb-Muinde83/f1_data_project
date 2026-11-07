{{ config(
    materialized = 'table'
) }}

{# Team is resolved PER SESSION via dim_driver_session, so a mid-season team change does not rewrite history. #}
WITH radio_data AS (
    SELECT * FROM {{ ref('stg_team_radio') }}
)

SELECT
    r.session_key,
    r.meeting_key,
    r.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    r.radio_timestamp,
    r.recording_url
FROM radio_data r
LEFT JOIN {{ ref('dim_driver_session') }} d
    ON r.session_key = d.session_key AND r.driver_number = d.driver_number
