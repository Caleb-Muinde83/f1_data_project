WITH raw_stints AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/stints_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(stint_number AS INTEGER) AS stint_number,
        compound AS tyre_compound,
        CAST(tyre_age_at_start AS INTEGER) AS tyre_age_at_start,
        CAST(lap_start AS INTEGER) AS lap_start,
        CAST(lap_end AS INTEGER) AS lap_end
    FROM raw_stints
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted