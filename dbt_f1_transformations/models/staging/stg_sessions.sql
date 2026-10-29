WITH raw_sessions AS (
    SELECT * FROM {{ source('openf1_raw', 'sessions') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(circuit_key AS {{ f1_type('int') }}) AS circuit_key,
        CAST(year AS {{ f1_type('int') }}) AS year,
        
        session_name,
        session_type,
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
        CAST(date_end AS {{ f1_type('timestamp') }}) AS date_end,
        gmt_offset
    FROM raw_sessions
    WHERE session_key IS NOT NULL
)
SELECT * FROM renamed_and_casted