WITH raw_location AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/location_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS location_timestamp,
        CAST(x AS DOUBLE) AS x_coordinate,
        CAST(y AS DOUBLE) AS y_coordinate,
        CAST(z AS DOUBLE) AS z_coordinate
    FROM raw_location
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted