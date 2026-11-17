{#
  Safety Car / Virtual Safety Car / Red Flag periods rebuilt from race control messages.

  Method: each status type gets its own ordered event stream. A period starts at a "start" event and ends at the
  next event of the same stream (or at the session end if no closing message exists).
    SC  : starts 'SAFETY CAR DEPLOYED',          ends 'SAFETY CAR IN THIS LAP'
    VSC : starts 'VIRTUAL SAFETY CAR DEPLOYED',  ends 'VIRTUAL SAFETY CAR ENDING'
    RED : starts on a track-wide RED flag,       ends on the next track-wide GREEN/CLEAR flag
  SUPERSEDING: a VSC that is upgraded to a Safety Car (or a red flag), or an SC followed by a red flag, ends when the MORE SEVERE period starts. Real
  data (2024 Shanghai): 'VIRTUAL SAFETY CAR DEPLOYED', then 'SAFETY CAR DEPLOYED' 4 minutes later and no 'VIRTUAL SAFETY CAR ENDING' at all.
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

starts AS (
    SELECT session_key, track_status, ts, next_ts,
           CASE track_status WHEN 'RED' THEN 1 WHEN 'SC' THEN 2 WHEN 'VSC' THEN 3 END AS severity_rank
    FROM streams WHERE is_start
),

superseded AS (
    SELECT a.session_key, a.track_status, a.ts, MIN(b.ts) AS superseded_ts
    FROM starts a
    JOIN starts b ON a.session_key = b.session_key AND b.ts > a.ts AND b.severity_rank < a.severity_rank
    GROUP BY a.session_key, a.track_status, a.ts
),

session_bounds AS (
    SELECT session_key, date_end AS session_end FROM {{ ref('stg_sessions') }}
)

SELECT
    s.session_key,
    s.track_status,
    s.severity_rank,
    s.ts AS period_start,
    {# End = the earliest of: its own closing message, the start of a more severe period, or (when neither exists) a cap instead of the session end
       (real data produced a 35-lap VSC). RED flags may be long. #}
    CASE
        WHEN u.superseded_ts IS NOT NULL
         AND u.superseded_ts < (CASE WHEN s.next_ts IS NOT NULL THEN s.next_ts
                                     WHEN s.track_status = 'VSC' THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_vsc_seconds', 600)) }}
                                     WHEN s.track_status = 'SC'  THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_sc_seconds', 1200)) }}
                                     ELSE COALESCE(b.session_end, s.ts) END)
        THEN u.superseded_ts
        WHEN s.next_ts IS NOT NULL THEN s.next_ts
        WHEN s.track_status = 'VSC' THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_vsc_seconds', 600)) }}
        WHEN s.track_status = 'SC'  THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_sc_seconds', 1200)) }}
        ELSE COALESCE(b.session_end, s.ts)
    END AS period_end,
    (s.next_ts IS NULL AND u.superseded_ts IS NULL) AS is_unclosed
FROM starts s
LEFT JOIN superseded u ON s.session_key = u.session_key AND s.track_status = u.track_status AND s.ts = u.ts
LEFT JOIN session_bounds b ON s.session_key = b.session_key
