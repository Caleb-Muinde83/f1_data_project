WITH raw_meetings AS (
    SELECT * FROM {{ source('openf1_raw', 'meetings') }}
),
renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(year AS {{ f1_type('int') }}) AS year,
        CAST(circuit_key AS {{ f1_type('int') }}) AS circuit_key,
        
        meeting_name,
        meeting_official_name,
        location,
        country_key,
        country_code,
        circuit_short_name,
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
        gmt_offset
    FROM raw_meetings
    WHERE meeting_key IS NOT NULL
)
SELECT * FROM renamed_and_casted