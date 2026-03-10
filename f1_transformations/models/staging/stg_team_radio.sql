WITH raw_team_radio AS (
    SELECT * FROM {{ source('openf1_raw', 'team_radio') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS radio_date,
        recording_url
        
    FROM raw_team_radio
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted