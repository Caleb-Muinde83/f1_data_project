{# Classification + championship points. Points follow the seed f1_points_system (current Race and Sprint scales);
   historical scoring systems are NOT modelled. The fastest-lap bonus applies to Race sessions up to
   var fastest_lap_bonus_last_year for drivers finishing in the top 10. DSQ/DNS score 0. #}
WITH results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),
grid AS (
    SELECT * FROM {{ ref('stg_starting_grid') }}
),
sessions AS (
    SELECT session_key, session_type, year, date_start AS session_start_time FROM {{ ref('stg_sessions') }}
),
fastest AS (
    SELECT session_key, driver_number FROM {{ ref('fct_fastest_laps') }} WHERE is_overall_fastest_lap
),
calc AS (
    SELECT
        r.session_key, r.meeting_key, r.driver_number,
        s.session_type, s.year, s.session_start_time,
        g.grid_position,
        r.position AS final_position,
        CASE WHEN g.grid_position IS NOT NULL AND r.position IS NOT NULL THEN g.grid_position - r.position END AS positions_gained_or_lost,
        CASE
            WHEN r.dsq THEN 'DSQ' WHEN r.dns THEN 'DNS' WHEN r.dnf THEN 'DNF'
            WHEN r.gap_to_leader_lapped_text IS NOT NULL THEN 'Lapped'
            ELSE 'Finished'
        END AS classification_status,
        r.number_of_laps AS laps_completed,
        r.race_time_seconds,
        r.gap_to_leader_seconds,
        r.gap_to_leader_lapped_text,
        r.q1_seconds, r.q2_seconds, r.q3_seconds,
        r.duration_raw AS race_duration,
        r.dns AS did_not_start,
        r.dsq AS disqualified,
        (f.driver_number IS NOT NULL) AS set_session_fastest_lap
    FROM results r
    LEFT JOIN grid g ON r.session_key = g.session_key AND r.driver_number = g.driver_number
    LEFT JOIN sessions s ON r.session_key = s.session_key
    LEFT JOIN fastest f ON r.session_key = f.session_key AND r.driver_number = f.driver_number
)
SELECT
    c.*,
    (c.final_position = 1) AS is_winner,
    (c.final_position <= 3) AS is_podium,
    CASE WHEN c.classification_status IN ('DSQ', 'DNS') THEN 0 ELSE COALESCE(p.points, 0) END AS position_points,
    CASE WHEN c.session_type = 'Race' AND c.set_session_fastest_lap AND c.final_position <= 10
              AND c.classification_status NOT IN ('DSQ', 'DNS') AND c.year <= {{ var('fastest_lap_bonus_last_year') }}
         THEN 1 ELSE 0 END AS fastest_lap_bonus_points,
    CASE WHEN c.classification_status IN ('DSQ', 'DNS') THEN 0 ELSE COALESCE(p.points, 0) END
      + CASE WHEN c.session_type = 'Race' AND c.set_session_fastest_lap AND c.final_position <= 10
                  AND c.classification_status NOT IN ('DSQ', 'DNS') AND c.year <= {{ var('fastest_lap_bonus_last_year') }}
             THEN 1 ELSE 0 END AS total_points
FROM calc c
LEFT JOIN {{ ref('f1_points_system') }} p
    ON c.session_type = p.session_type AND c.final_position = p.position
