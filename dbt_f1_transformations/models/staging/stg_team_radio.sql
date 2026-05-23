WITH raw_team_radio AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/team_radio_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS radio_timestamp,
        recording_url
    FROM raw_team_radio
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted