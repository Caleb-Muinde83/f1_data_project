"""Read-only data access for the dashboards. Every function takes an open DuckDB connection and returns a DataFrame."""
import os
import duckdb
import pandas as pd

DEFAULT_DB = os.environ.get("F1_DUCKDB_PATH", "f1_local_warehouse.duckdb")


def connect(path: str = None):
    """Open the warehouse READ-ONLY. DuckDB allows one writer: if dbt is running you get a clear error, not a hang."""
    path = path or DEFAULT_DB
    if not os.path.exists(path):
        raise FileNotFoundError(f"Warehouse not found at {path}. Set F1_DUCKDB_PATH or build it with dbt first.")
    try:
        return duckdb.connect(path, read_only=True)
    except duckdb.IOException as exc:
        raise RuntimeError("The warehouse is locked by another process (probably a dbt run). Retry when it finishes.") from exc


def q(con, sql: str, params=None) -> pd.DataFrame:
    return con.execute(sql, params or []).df()


def sessions(con, race_only: bool = False):
    where = "WHERE session_type IN ('Race', 'Sprint')" if race_only else ""
    return q(con, f"SELECT session_key, year, session_name, session_type, location, circuit_short_name, session_start_time "
                  f"FROM dim_sessions {where} ORDER BY session_start_time DESC")


def label(row) -> str:
    return f"{row.year} {row.location} | {row.session_name} ({row.session_key})"


def drivers(con, session_key: int):
    return q(con, "SELECT driver_number, driver_acronym, full_name, team_name, team_color_hex FROM dim_driver_session WHERE session_key = ? ORDER BY driver_number", [session_key])


def results(con, session_key: int):
    return q(con, """SELECT r.final_position AS pos, d.driver_acronym AS driver, d.team_name AS team, r.grid_position AS grid,
        r.positions_gained_or_lost AS gained, r.classification_status AS status, r.laps_completed AS laps,
        r.total_points AS points, r.q1_seconds, r.q2_seconds, r.q3_seconds
        FROM fct_race_results r LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE r.session_key = ? ORDER BY r.final_position""", [session_key])


def race_trace(con, session_key: int):
    return q(con, """SELECT t.lap_number, d.driver_acronym AS driver, t.gap_to_leader_by_cumulative_time_seconds AS gap_s,
        t.position_by_cumulative_time AS position, t.tyre_compound, t.track_status
        FROM fct_race_trace t LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE t.session_key = ? ORDER BY t.lap_number, t.position_by_cumulative_time""", [session_key])


def lap_pace(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, l.lap_number, l.lap_time_seconds, l.fuel_corrected_lap_time_seconds,
        l.tyre_compound, l.tyre_age_this_lap, l.track_status, l.is_representative_lap
        FROM fct_lap_performance l LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE l.session_key = ? AND l.lap_time_seconds IS NOT NULL""", [session_key])


def stints(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, s.stint_number, s.tyre_compound, s.first_lap, s.last_lap,
        s.stint_length_laps, s.avg_representative_lap_seconds, s.degradation_seconds_per_lap
        FROM fct_stints s LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE s.session_key = ? ORDER BY d.driver_acronym, s.stint_number""", [session_key])


def pit_stops(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, p.stop_number, p.lap_number, p.compound_before, p.compound_after,
        p.pit_lane_time_seconds, p.time_lost_seconds, p.net_positions_gained, p.is_under_neutralisation, p.is_slow_stop
        FROM fct_pit_stops p LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE p.session_key = ? ORDER BY p.lap_number, d.driver_acronym""", [session_key])


def undercuts(con, session_key: int):
    return q(con, """SELECT a.driver_acronym AS attacker, dd.driver_acronym AS defender, u.strategy_type, u.attacker_stop_lap,
        u.defender_stop_lap, u.gap_before_seconds, u.gap_after_seconds, u.gap_swing_seconds, u.attacker_ahead_after, u.any_neutralisation
        FROM fct_undercut_overcut u
        LEFT JOIN dim_driver_session a ON a.session_key = u.session_key AND a.driver_number = u.attacker_driver_number
        LEFT JOIN dim_driver_session dd ON dd.session_key = u.session_key AND dd.driver_number = u.defender_driver_number
        WHERE u.session_key = ? ORDER BY u.attacker_stop_lap""", [session_key])


def points_progression(con, year: int):
    return q(con, """SELECT d.driver_acronym AS driver, p.session_start_time, p.cumulative_points, p.championship_position_after_session AS position
        FROM fct_points_progression p LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE p.year = ? ORDER BY p.session_start_time""", [year])


def delta_trace(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, t.distance_m, t.time_delta_seconds, t.speed_delta_kph
        FROM fct_lap_time_delta_trace t LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE t.session_key = ? ORDER BY t.driver_number, t.distance_m""", [session_key])


def corners(con, session_key: int):
    return q(con, """SELECT c.corner_number, c.apex_distance_m, c.reference_apex_speed_kph, c.turn_angle_deg, c.turn_direction, c.speed_class,
        d.driver_acronym AS driver, ROUND(AVG(p.apex_speed_kph), 1) AS avg_apex_kph, ROUND(AVG(p.entry_speed_kph), 1) AS avg_entry_kph,
        ROUND(AVG(p.brake_to_apex_distance_m), 0) AS avg_brake_to_apex_m
        FROM dim_session_corners c
        LEFT JOIN fct_corner_performance p ON p.session_key = c.session_key AND p.corner_number = c.corner_number AND p.is_representative_lap
        LEFT JOIN dim_driver_session d ON d.session_key = p.session_key AND d.driver_number = p.driver_number
        WHERE c.session_key = ? GROUP BY ALL ORDER BY c.corner_number, d.driver_acronym""", [session_key])


def sc_hazard(con, circuit_key: int = None):
    where = "WHERE circuit_key = ?" if circuit_key is not None else ""
    return q(con, f"SELECT circuit_key, track_status, laps_remaining, p_at_least_one_start, races_observed, low_sample "
                  f"FROM fct_safety_car_hazard {where} ORDER BY circuit_key, track_status, laps_remaining",
             [circuit_key] if circuit_key is not None else None)


def radio(con, session_key: int):
    return q(con, """SELECT radio_timestamp, driver_name, lap_number, tyre_compound, track_status, transcript,
        mentions_pit_call, mentions_tyres, mentions_neutralisation, mentions_drs, mentions_penalty, mentions_power_unit
        FROM fct_radio_messages WHERE session_key = ? ORDER BY radio_timestamp""", [session_key])
