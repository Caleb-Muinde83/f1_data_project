WITH raw_stints AS (
    SELECT * FROM {{ source('openf1_raw', 'stints') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(stint_number AS {{ f1_type('int') }}) AS stint_number,
        compound AS tyre_compound,
        CAST(tyre_age_at_start AS {{ f1_type('int') }}) AS tyre_age_at_start,
        CAST(lap_start AS {{ f1_type('int') }}) AS lap_start,
        CAST(lap_end AS {{ f1_type('int') }}) AS lap_end
    FROM raw_stints
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted