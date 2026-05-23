WITH raw_intervals AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/intervals_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        interval,
        gap_to_leader
    FROM raw_intervals
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted