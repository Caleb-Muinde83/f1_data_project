WITH raw_championship_teams AS (
    SELECT * FROM read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/championship_teams_*_extracted.parquet')
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        team_name,
        position_start,
        position_current,
        CAST(points_start AS DOUBLE) AS points_start,
        CAST(points_current AS DOUBLE) AS points_current
    FROM raw_championship_teams
)
SELECT * FROM renamed_and_casted