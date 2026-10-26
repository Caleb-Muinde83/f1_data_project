-- Example race-engineer / analyst queries. Run against f1_local_warehouse.duckdb (DuckDB CLI, Harlequin, DBeaver).
-- Pace work: ALWAYS filter is_representative_lap and prefer fuel_corrected_lap_time_seconds in races.

-- 1. Race pace ranking (fuel-corrected, clean laps only)
SELECT driver_number,
       COUNT(*)                                   AS clean_laps,
       ROUND(AVG(fuel_corrected_lap_time_seconds), 3) AS avg_pace_s,
       ROUND(MIN(lap_time_seconds), 3)            AS best_clean_lap_s
FROM fct_lap_performance
WHERE session_key = 9002 AND is_representative_lap
GROUP BY driver_number ORDER BY avg_pace_s;

-- 2. Tyre degradation by compound (positive = losing time per lap of tyre age)
SELECT tyre_compound,
       COUNT(*)                                   AS stints_measured,
       ROUND(AVG(degradation_seconds_per_lap), 4) AS avg_deg_s_per_lap
FROM fct_stints
WHERE degradation_seconds_per_lap IS NOT NULL
GROUP BY tyre_compound ORDER BY avg_deg_s_per_lap DESC;

-- 3. Strategy comparison for a race
SELECT driver_number, strategy, pit_stop_count, stint_lengths
FROM fct_tire_strategy WHERE session_key = 9002 ORDER BY pit_stop_count, driver_number;

-- 4. Pit stops: what did each one actually cost? (green-flag stops only)
SELECT driver_number, stop_number, lap_number, compound_before, compound_after,
       pit_lane_time_seconds, ROUND(time_lost_seconds, 2) AS time_lost_s, net_positions_gained
FROM fct_pit_stops WHERE NOT is_under_neutralisation ORDER BY time_lost_s DESC;

-- 5. Who spent the most race time in dirty air (within var dirty_air_seconds of the car ahead)?
SELECT driver_number,
       ROUND(SUM(seconds_in_dirty_air) / NULLIF(SUM(covered_seconds), 0) * 100, 1) AS pct_race_in_dirty_air,
       ROUND(SUM(seconds_in_drs_range), 0) AS seconds_in_drs_range
FROM fct_lap_gaps WHERE session_key = 9002 GROUP BY driver_number ORDER BY pct_race_in_dirty_air DESC;

-- 6. Race trace: gap to the leader every lap
SELECT lap_number, driver_number, ROUND(gap_to_leader_by_cumulative_time_seconds, 2) AS gap_s, tyre_compound, track_status
FROM fct_race_trace WHERE session_key = 9002 ORDER BY lap_number, position_by_cumulative_time;

-- 7. Where is each driver losing time to the fastest lap? (delta trace; > 0 = behind)
SELECT driver_number, distance_m, ROUND(time_delta_seconds, 3) AS delta_s, ROUND(speed_delta_kph, 1) AS speed_delta_kph
FROM fct_lap_time_delta_trace WHERE session_key = 9002 AND driver_number = 44 ORDER BY distance_m;

-- 8. Quali: time left on the table (best lap vs sum of best sectors)
SELECT driver_number, ROUND(fastest_lap_time_seconds, 3) AS best, ROUND(theoretical_best_lap_seconds, 3) AS theoretical,
       ROUND(time_left_on_table_seconds, 3) AS left_on_table_s
FROM fct_fastest_laps WHERE session_key = 9001 ORDER BY session_rank;

-- 9. Championship picture after each round (computed from results, not the beta endpoint)
SELECT session_key, driver_number, cumulative_points, gap_to_points_leader, championship_position_after_session
FROM fct_points_progression WHERE year = 2024 ORDER BY session_key, championship_position_after_session;

-- 10. Real passes only (exclude pit-cycle position changes and safety-car periods)
SELECT overtaking_driver_number, overtaken_driver_number, lap_number, tyre_age_delta, was_in_drs_range_before_pass
FROM fct_overtakes WHERE NOT is_probable_pit_cycle_pass AND track_status = 'GREEN';
