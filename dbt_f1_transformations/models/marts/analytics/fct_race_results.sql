{# Classification + championship points. Points follow the SEASON-AWARE seed f1_points_system (season, session_type, position),
   so a Sprint in 2021 scores 3-2-1 and a Sprint in 2023 scores 8-1. Seasons missing from the seed score 0 and are caught by
   tests/assert_every_season_has_points_scale.sql. The fastest-lap bonus applies to Race sessions up to
   var fastest_lap_bonus_last_year for drivers finishing in the top 10. DSQ/DNS score 0. #}
WITH results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),
grid AS (
    SELECT * FROM {{ ref('stg_starting_grid') }}
),
sessions AS (
    SELECT session_key, meeting_key, session_name, session_type, year, date_start AS session_start_time FROM {{ ref('stg_sessions') }}
),
{# OpenF1 can key starting_grid rows to the session that SET the grid (Qualifying for a Race, Sprint Qualifying/Shootout for a Sprint)
   instead of the race itself. grid_session maps each race to that session; the direct match always wins. #}
grid_session AS (
    SELECT session_key, grid_session_key FROM (
        SELECT s.session_key, q.session_key AS grid_session_key,
               ROW_NUMBER() OVER (PARTITION BY s.session_key ORDER BY q.session_start_time DESC) AS rn
        FROM sessions s
        JOIN sessions q ON q.meeting_key = s.meeting_key AND q.session_start_time < s.session_start_time
         AND ((s.session_type = 'Race' AND q.session_name = 'Qualifying')
           OR (s.session_type = 'Sprint' AND q.session_name IN ('Sprint Qualifying', 'Sprint Shootout')))
        WHERE s.session_type IN ('Race', 'Sprint')
    ) m WHERE rn = 1
),
fastest AS (
    SELECT session_key, driver_number FROM {{ ref('fct_fastest_laps') }} WHERE is_overall_fastest_lap
),
calc AS (
    SELECT
        r.session_key, r.meeting_key, r.driver_number,
        s.session_type, s.year, s.session_start_time,
        COALESCE(g1.grid_position, g2.grid_position) AS grid_position,
        CASE WHEN g1.grid_position IS NOT NULL THEN 'starting_grid' WHEN g2.grid_position IS NOT NULL THEN 'starting_grid_via_qualifying' END AS grid_source,
        r.position AS final_position,
        CASE WHEN COALESCE(g1.grid_position, g2.grid_position) IS NOT NULL AND r.position IS NOT NULL
             THEN COALESCE(g1.grid_position, g2.grid_position) - r.position END AS positions_gained_or_lost,
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
    LEFT JOIN grid g1 ON r.session_key = g1.session_key AND r.driver_number = g1.driver_number
    LEFT JOIN grid_session gs ON r.session_key = gs.session_key
    LEFT JOIN grid g2 ON gs.grid_session_key = g2.session_key AND r.driver_number = g2.driver_number
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
    ON c.year = p.season AND c.session_type = p.session_type AND c.final_position = p.position
