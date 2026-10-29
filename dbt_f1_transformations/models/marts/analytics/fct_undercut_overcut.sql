{#
  One row per pair of related pit stops where the ATTACKER was running directly behind the DEFENDER.
    UNDERCUT : attacker stopped first (fresh tyres, hoping to emerge ahead)
    OVERCUT  : attacker stayed out longer (hoping the defender loses time on cold tyres / in traffic)
    SAME_LAP : both stopped on the same lap (not a strategy contest)
  attacker_ahead_after = position by cumulative race time one lap after BOTH stops have completed.
  Limits: pairs only if adjacent before the first stop and within var undercut_max_gap_seconds; gap/position come from
  lap-end cumulative times, so a position swap that happens and reverses inside one lap is invisible. Stops under
  SC/VSC are kept but flagged (any_neutralisation) because the pit-loss maths changes completely there.
#}
WITH stops AS (
    SELECT session_key, driver_number, stop_number, lap_number, tyre_age_before, is_under_neutralisation
    FROM {{ ref('fct_pit_stops') }}
),
trace AS (
    SELECT session_key, driver_number, lap_number,
           position_by_cumulative_time AS pos, cumulative_race_time_seconds AS cum
    FROM {{ ref('fct_race_trace') }}
),
pairs AS (
    SELECT
        a.session_key,
        a.driver_number AS attacker_driver_number, a.stop_number AS attacker_stop_number, a.lap_number AS attacker_stop_lap,
        a.tyre_age_before AS attacker_tyre_age_at_stop,
        d.driver_number AS defender_driver_number, d.stop_number AS defender_stop_number, d.lap_number AS defender_stop_lap,
        d.tyre_age_before AS defender_tyre_age_at_stop,
        (a.is_under_neutralisation OR d.is_under_neutralisation) AS any_neutralisation,
        LEAST(a.lap_number, d.lap_number) - 1 AS lap_before,
        GREATEST(a.lap_number, d.lap_number) + 1 AS lap_after
    FROM stops a
    JOIN stops d
        ON a.session_key = d.session_key
        AND a.driver_number <> d.driver_number
        AND ABS(a.lap_number - d.lap_number) <= {{ var('undercut_window_laps') }}
),
positioned AS (
    SELECT
        p.*,
        ta.pos AS attacker_position_before, td.pos AS defender_position_before,
        ta.cum - td.cum AS gap_before_seconds,
        tae.pos AS attacker_position_after, tde.pos AS defender_position_after,
        tae.cum - tde.cum AS gap_after_seconds
    FROM pairs p
    JOIN trace ta ON p.session_key = ta.session_key AND p.attacker_driver_number = ta.driver_number AND p.lap_before = ta.lap_number
    JOIN trace td ON p.session_key = td.session_key AND p.defender_driver_number = td.driver_number AND p.lap_before = td.lap_number
    LEFT JOIN trace tae ON p.session_key = tae.session_key AND p.attacker_driver_number = tae.driver_number AND p.lap_after = tae.lap_number
    LEFT JOIN trace tde ON p.session_key = tde.session_key AND p.defender_driver_number = tde.driver_number AND p.lap_after = tde.lap_number
)
SELECT
    session_key,
    attacker_driver_number, attacker_stop_number, attacker_stop_lap, attacker_tyre_age_at_stop,
    defender_driver_number, defender_stop_number, defender_stop_lap, defender_tyre_age_at_stop,
    CASE WHEN attacker_stop_lap < defender_stop_lap THEN 'UNDERCUT'
         WHEN attacker_stop_lap > defender_stop_lap THEN 'OVERCUT'
         ELSE 'SAME_LAP' END AS strategy_type,
    attacker_position_before, defender_position_before,
    gap_before_seconds,
    attacker_position_after, defender_position_after,
    gap_after_seconds,
    gap_before_seconds - gap_after_seconds AS gap_swing_seconds,
    attacker_position_after < defender_position_after AS attacker_ahead_after,
    any_neutralisation
FROM positioned
WHERE attacker_position_before = defender_position_before + 1
  AND gap_before_seconds <= {{ var('undercut_max_gap_seconds') }}
