{# One row per driver per session: best lap, personal-best sectors, theoretical best lap and session ranking. #}
WITH valid AS (
    SELECT * FROM {{ ref('int_lap_context') }}
    WHERE lap_time_seconds IS NOT NULL AND lap_time_seconds > 0
),
best_lap AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY lap_time_seconds ASC, lap_number) AS lap_rank
    FROM valid
),
best_sectors AS (
    SELECT
        session_key, driver_number,
        MIN(duration_sector_1) AS best_sector_1_seconds,
        MIN(duration_sector_2) AS best_sector_2_seconds,
        MIN(duration_sector_3) AS best_sector_3_seconds
    FROM valid
    GROUP BY 1, 2
),
joined AS (
    SELECT
        b.session_key, b.meeting_key, b.driver_number, b.lap_number,
        b.lap_time_seconds AS fastest_lap_time_seconds,
        b.tyre_compound, b.tyre_age_this_lap, b.speed_trap_finish,
        s.best_sector_1_seconds, s.best_sector_2_seconds, s.best_sector_3_seconds,
        s.best_sector_1_seconds + s.best_sector_2_seconds + s.best_sector_3_seconds AS theoretical_best_lap_seconds
    FROM best_lap b
    JOIN best_sectors s ON b.session_key = s.session_key AND b.driver_number = s.driver_number
    WHERE b.lap_rank = 1
)
SELECT
    *,
    fastest_lap_time_seconds - theoretical_best_lap_seconds AS time_left_on_table_seconds,
    RANK() OVER (PARTITION BY session_key ORDER BY fastest_lap_time_seconds) AS session_rank,
    fastest_lap_time_seconds - MIN(fastest_lap_time_seconds) OVER (PARTITION BY session_key) AS gap_to_session_fastest_seconds,
    (fastest_lap_time_seconds = MIN(fastest_lap_time_seconds) OVER (PARTITION BY session_key)) AS is_overall_fastest_lap
FROM joined
