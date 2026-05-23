WITH sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

meetings AS (
    SELECT * FROM {{ ref('stg_meetings') }}
),

enriched_sessions AS (
    SELECT
        -- Session specific details
        s.session_key,
        s.session_name,
        s.session_type,
        s.date_start AS session_start_time,
        s.date_end AS session_end_time,
        
        -- Meeting & Location details brought in from the join
        s.meeting_key,
        m.meeting_name,
        m.meeting_official_name,
        s.year,
        m.circuit_key,
        m.circuit_short_name,
        m.location,
        m.country_code,
        s.gmt_offset
        
    FROM sessions s
    LEFT JOIN meetings m
        ON s.meeting_key = m.meeting_key
)

SELECT * FROM enriched_sessions