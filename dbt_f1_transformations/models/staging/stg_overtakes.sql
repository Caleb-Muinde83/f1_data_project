WITH raw_overtakes AS (
    SELECT * FROM {{ source('openf1_raw', 'overtakes') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(date AS {{ f1_type('timestamp') }}) AS overtake_timestamp,

        CAST(overtaking_driver_number AS {{ f1_type('int') }}) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS {{ f1_type('int') }}) AS overtaken_driver_number,
        CAST(position AS {{ f1_type('int') }}) AS position
    FROM raw_overtakes
)
SELECT * FROM renamed_and_casted
