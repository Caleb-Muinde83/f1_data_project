{#
  Safety Car / Virtual Safety Car / Red Flag periods rebuilt from race control messages.

  Method: each status type gets its own ordered event stream. A period starts at a "start" event and ends at the
  next event of the same stream (or at the session end if no closing message exists).
    SC  : starts 'SAFETY CAR DEPLOYED',          ends 'SAFETY CAR IN THIS LAP'
    VSC : starts 'VIRTUAL SAFETY CAR DEPLOYED',  ends 'VIRTUAL SAFETY CAR ENDING'
    RED : starts on a track-wide RED flag,       ends on the next track-wide GREEN/CLEAR flag
  APPROXIMATION: an SC period ends when the "in this lap" message is issued, so the lap in which the message is
  issued is still labelled SC (it is the last lap behind the car). Restart-lap effects are not modelled.
#}
WITH rc AS (
    SELECT * FROM {{ ref('stg_race_control') }}
),

classified AS (
    SELECT
        session_key,
        race_control_timestamp AS ts,
        CASE
            WHEN upper(message) LIKE 'VIRTUAL SAFETY CAR%' THEN 'VSC'
            WHEN upper(message) LIKE 'SAFETY CAR%' THEN 'SC'
            WHEN upper(flag) = 'RED' AND coalesce(upper(scope), 'TRACK') = 'TRACK' THEN 'RED'
            WHEN upper(flag) IN ('GREEN', 'CLEAR') AND coalesce(upper(scope), 'TRACK') = 'TRACK' THEN 'RED'
        END AS track_status,
        CASE
            WHEN upper(message) LIKE '%DEPLOYED%' THEN TRUE
            WHEN upper(flag) = 'RED' THEN TRUE
            ELSE FALSE
        END AS is_start
    FROM rc
    WHERE race_control_timestamp IS NOT NULL
),

streams AS (
    SELECT
        *,
        LEAD(ts) OVER (PARTITION BY session_key, track_status ORDER BY ts) AS next_ts
    FROM classified
    WHERE track_status IS NOT NULL
),

session_bounds AS (
    SELECT session_key, date_end AS session_end FROM {{ ref('stg_sessions') }}
)

SELECT
    s.session_key,
    s.track_status,
    CASE s.track_status WHEN 'RED' THEN 1 WHEN 'SC' THEN 2 WHEN 'VSC' THEN 3 END AS severity_rank,
    s.ts AS period_start,
    COALESCE(s.next_ts, b.session_end, s.ts) AS period_end
FROM streams s
LEFT JOIN session_bounds b ON s.session_key = b.session_key
WHERE s.is_start
