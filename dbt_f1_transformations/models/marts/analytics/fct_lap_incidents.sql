{# Lap-level incident view. Race control messages are attached to laps by TIME WINDOW (lap start to next lap
   start), not by the often-empty lap_number column. primary_flag is ranked by severity, not alphabetically. #}
WITH events AS (
    SELECT
        l.session_key, l.driver_number, l.lap_number,
        rc.category, rc.flag, rc.message,
        CASE upper(rc.flag)
            WHEN 'RED' THEN 1 WHEN 'DOUBLE YELLOW' THEN 2 WHEN 'YELLOW' THEN 3
            WHEN 'BLACK AND WHITE' THEN 4 WHEN 'BLACK' THEN 4 WHEN 'BLUE' THEN 5 ELSE 9
        END AS flag_rank
    FROM {{ ref('int_lap_context') }} l
    JOIN {{ ref('stg_race_control') }} rc
        ON l.session_key = rc.session_key
        AND rc.race_control_timestamp >= l.lap_start_ts
        AND rc.race_control_timestamp < l.lap_end_ts
        AND (rc.driver_number IS NULL OR rc.driver_number = l.driver_number)
),
per_lap AS (
    SELECT
        session_key, driver_number, lap_number,
        STRING_AGG(DISTINCT category, ', ' ORDER BY category) AS incident_categories,
        COUNT(*) AS race_control_message_count,
        MIN(flag_rank) AS worst_flag_rank,
        {{ f1_bool_or("upper(message) LIKE '%PENALTY%'") }} AS has_penalty_message
    FROM events
    GROUP BY 1, 2, 3
)
SELECT
    l.session_key, l.meeting_key, l.driver_number, l.lap_number,
    l.lap_time_seconds,
    l.track_status,
    l.neutralisation_status,
    (l.track_status <> 'GREEN') AS is_affected_by_incident,
    p.incident_categories,
    COALESCE(p.race_control_message_count, 0) AS race_control_message_count,
    CASE p.worst_flag_rank WHEN 1 THEN 'RED' WHEN 2 THEN 'DOUBLE YELLOW' WHEN 3 THEN 'YELLOW'
        WHEN 4 THEN 'BLACK AND WHITE' WHEN 5 THEN 'BLUE' END AS primary_flag,
    COALESCE(p.has_penalty_message, FALSE) AS has_penalty_message
FROM {{ ref('int_lap_context') }} l
LEFT JOIN per_lap p
    ON l.session_key = p.session_key AND l.driver_number = p.driver_number AND l.lap_number = p.lap_number
