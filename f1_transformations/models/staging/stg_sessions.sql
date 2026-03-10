WITH raw_sessions AS (
    SELECT * FROM {{ source('openf1_raw', 'sessions') }}
),

renamed_and_casted AS (
    SELECT
        -- IDs and Keys
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(circuit_key AS INTEGER) AS circuit_key,
        CAST(country_key AS INTEGER) AS country_key,
        CAST(year AS INTEGER) AS session_year,

        -- Timestamps
        CAST(date_start AS TIMESTAMP) AS date_start,
        CAST(date_end AS TIMESTAMP) AS date_end,
        gmt_offset,

        -- Session Details
        session_type,
        session_name,

        -- Location Details
        circuit_short_name,
        country_code,
        country_name,
        location

    FROM raw_sessions
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted