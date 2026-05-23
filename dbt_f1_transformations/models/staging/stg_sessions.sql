WITH raw_sessions AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/sessions_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(circuit_key AS INTEGER) AS circuit_key,
        CAST(year AS INTEGER) AS year,
        
        session_name,
        session_type,
        CAST(date_start AS TIMESTAMP) AS date_start,
        CAST(date_end AS TIMESTAMP) AS date_end,
        gmt_offset
    FROM raw_sessions
    WHERE session_key IS NOT NULL
)
SELECT * FROM renamed_and_casted