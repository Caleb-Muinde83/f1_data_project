WITH raw_laps AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/laps_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(lap_number AS INTEGER) AS lap_number,
        lap_duration,
        
        -- The actual time spent in each sector
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        
        -- The micro-sector arrays (useful for detailed telemetry later)
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        
        is_pit_out_lap
        
    FROM raw_laps
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted