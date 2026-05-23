WITH raw_car_data AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/car_data_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS telemetry_timestamp,
        CAST(rpm AS INTEGER) AS rpm,
        CAST(speed AS INTEGER) AS speed,
        CAST(n_gear AS INTEGER) AS gear,
        CAST(throttle AS INTEGER) AS throttle,
        CAST(brake AS INTEGER) AS brake,
        CAST(drs AS INTEGER) AS drs
    FROM raw_car_data
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted