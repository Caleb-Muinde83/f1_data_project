WITH raw_pit AS (
    SELECT * FROM {{ source('openf1_raw', 'pit') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(lap_number AS {{ f1_type('int') }}) AS lap_number,
        CAST(date AS {{ f1_type('timestamp') }}) AS pit_timestamp,
        {{ f1_try_cast_fn() }}(pit_duration AS {{ f1_type('double') }}) AS pit_duration
    FROM raw_pit
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
