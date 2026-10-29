WITH raw_race_control AS (
    SELECT * FROM {{ source('openf1_raw', 'race_control') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        -- TRY_CAST: an all-empty Parquet column can be typed as text, which would crash a plain CAST
        {{ f1_try_cast_fn() }}(lap_number AS {{ f1_type('int') }}) AS lap_number,
        {{ f1_try_cast_fn() }}(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(date AS {{ f1_type('timestamp') }}) AS race_control_timestamp,
        category,
        message,
        flag,
        scope,
        {{ f1_try_cast_fn() }}(sector AS {{ f1_type('int') }}) AS sector
    FROM raw_race_control
)
SELECT * FROM renamed_and_casted
