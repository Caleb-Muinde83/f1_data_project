"""Read-only data access for the dashboards. Every function takes an open DuckDB connection and returns a DataFrame."""
import os
import duckdb
import pandas as pd

DEFAULT_DB = os.environ.get("F1_DUCKDB_PATH", "f1_local_warehouse.duckdb")


CURRENT_DB = None      # path of the warehouse opened by connect(), shown in the sidebar


def connect(path: str = None):
    """Open the warehouse READ-ONLY. DuckDB allows one writer: if dbt is running you get a clear error, not a hang."""
    path = path or DEFAULT_DB
    if not os.path.exists(path):
        raise FileNotFoundError(f"Warehouse not found at {path}. Set F1_DUCKDB_PATH or build it with dbt first.")
    global CURRENT_DB
    CURRENT_DB = path
    try:
        return duckdb.connect(path, read_only=True)
    except duckdb.IOException as exc:
        raise RuntimeError("The warehouse is locked by another process (probably a dbt run). Retry when it finishes.") from exc


def q(con, sql: str, params=None) -> pd.DataFrame:
    """Run a query and return a DataFrame. Floats are rounded to 6 decimals: it removes display noise such as 1e-13 'time left on table'."""
    df = con.execute(sql, params or []).df()
    fcols = df.select_dtypes("float").columns
    df[fcols] = df[fcols].round(6)
    return df


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
        LEFT JOIN fct_corner_performance p ON p.session_key = c.session_key AND p.corner_number = c.corner_number AND p.is_representative_lap AND p.is_complete_lap
        LEFT JOIN dim_driver_session d ON d.session_key = p.session_key AND d.driver_number = p.driver_number
        WHERE c.session_key = ? GROUP BY ALL ORDER BY c.corner_number, d.driver_acronym""", [session_key])


def sc_hazard(con, circuit_key: int = None):
    where = "WHERE circuit_key = ?" if circuit_key is not None else ""
    return q(con, f"SELECT circuit_key, track_status, laps_remaining, p_at_least_one_start, races_observed, low_sample "
                  f"FROM fct_safety_car_hazard {where} ORDER BY circuit_key, track_status, laps_remaining",
             [circuit_key] if circuit_key is not None else None)


def radio(con, session_key: int):
    return q(con, """SELECT radio_timestamp, driver_number, driver_name, lap_number, tyre_compound, track_status, transcript,
        mentions_pit_call, mentions_tyres, mentions_neutralisation, mentions_drs, mentions_penalty, mentions_power_unit
        FROM fct_radio_messages WHERE session_key = ? ORDER BY radio_timestamp""", [session_key])


# ---------------------------------------------------------------------------------------------------------------------
# Context and session navigation (season -> Grand Prix -> session)
# ---------------------------------------------------------------------------------------------------------------------
def meetings(con):
    return q(con, """SELECT year, meeting_key, MAX(meeting_name) AS meeting_name, MAX(location) AS location, MAX(circuit_key) AS circuit_key,
        MIN(session_start_time) AS first_session FROM dim_sessions GROUP BY year, meeting_key ORDER BY first_session DESC""")


def meeting_sessions(con, meeting_key: int):
    return q(con, "SELECT session_key, session_name, session_type, session_start_time, year, circuit_key, location FROM dim_sessions "
                  "WHERE meeting_key = ? ORDER BY session_start_time", [meeting_key])


def session_row(con, session_key: int):
    df = q(con, "SELECT session_key, meeting_key, year, circuit_key, circuit_short_name, location, session_name, session_type, session_start_time "
                "FROM dim_sessions WHERE session_key = ?", [session_key])
    return None if df.empty else df.iloc[0]


def session_laps(con, session_key: int) -> int:
    df = q(con, "SELECT MAX(lap_number) AS n FROM fct_lap_performance WHERE session_key = ?", [session_key])
    return int(df.iloc[0, 0]) if not df.empty and pd.notna(df.iloc[0, 0]) else 0       # a session with no laps gives NA, not 0


def session_has(con, table: str, session_key: int) -> bool:
    return int(q(con, f"SELECT COUNT(*) FROM {table} WHERE session_key = ?", [session_key]).iloc[0, 0]) > 0


# ---------------------------------------------------------------------------------------------------------------------
# Pre-race: ONLY information that existed before the race started (no leakage)
# ---------------------------------------------------------------------------------------------------------------------
def prior_sessions(con, meeting_key: int, before_ts):
    """Sessions of the same Grand Prix weekend that started before the target race."""
    return q(con, "SELECT session_key, session_name, session_type, session_start_time FROM dim_sessions "
                  "WHERE meeting_key = ? AND session_start_time < ? ORDER BY session_start_time", [meeting_key, before_ts])


def prior_races_at_circuit(con, circuit_key: int, before_ts):
    return q(con, """SELECT s.year, s.session_key, s.session_name, s.session_start_time, d.driver_acronym AS winner, d.team_name AS winning_team
        FROM dim_sessions s
        JOIN fct_race_results r ON r.session_key = s.session_key AND r.final_position = 1
        LEFT JOIN dim_driver_session d ON d.session_key = r.session_key AND d.driver_number = r.driver_number
        WHERE s.circuit_key = ? AND s.session_type IN ('Race', 'Sprint') AND s.session_start_time < ? ORDER BY s.session_start_time DESC""",
             [circuit_key, before_ts])


def qualifying_results(con, session_keys):
    if not len(session_keys):
        return pd.DataFrame()
    ph = ",".join("?" * len(session_keys))
    return q(con, f"""SELECT s.session_name, r.final_position AS pos, d.driver_acronym AS driver, d.team_name AS team,
        r.q1_seconds, r.q2_seconds, r.q3_seconds
        FROM fct_race_results r JOIN dim_sessions s USING (session_key)
        LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE r.session_key IN ({ph}) AND s.session_type = 'Qualifying' ORDER BY s.session_start_time, r.final_position""", list(session_keys))


def grid_finish_history(con, before_ts):
    """(grid, finish) pairs of completed races BEFORE the target race: the base rate for finishing-position probabilities."""
    return q(con, """SELECT r.session_key, r.grid_position, r.final_position, r.classification_status
        FROM fct_race_results r JOIN dim_sessions s USING (session_key)
        WHERE s.session_type = 'Race' AND s.session_start_time < ? AND r.grid_position IS NOT NULL""", [before_ts])


def starting_grid(con, session_key: int):
    return q(con, """SELECT r.grid_position AS grid, d.driver_acronym AS driver, d.team_name AS team, r.driver_number
        FROM fct_race_results r LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE r.session_key = ? AND r.grid_position IS NOT NULL ORDER BY r.grid_position""", [session_key])


def compound_pace(con, session_keys):
    if not len(session_keys):
        return pd.DataFrame()
    ph = ",".join("?" * len(session_keys))
    return q(con, f"""SELECT c.session_key, s.session_name, d.driver_acronym AS driver, c.tyre_compound, c.clean_laps, c.fresh_tyre_pace_seconds,
        c.degradation_seconds_per_lap, c.r_squared, c.min_tyre_age, c.max_tyre_age
        FROM fct_compound_pace_model c JOIN dim_sessions s USING (session_key)
        LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE c.session_key IN ({ph}) ORDER BY s.session_start_time, d.driver_acronym, c.tyre_compound""", list(session_keys))


def weather_by_lap(con, session_keys):
    if not len(session_keys):
        return pd.DataFrame()
    ph = ",".join("?" * len(session_keys))
    return q(con, f"""SELECT s.session_name, w.session_key, w.lap_number, AVG(w.air_temperature) AS air_temp, AVG(w.track_temperature) AS track_temp,
        AVG(w.humidity) AS humidity, AVG(w.wind_speed) AS wind_speed, MAX(CASE WHEN w.is_raining_at_lap_start THEN 1 ELSE 0 END) AS raining
        FROM fct_weather_impact w JOIN dim_sessions s USING (session_key)
        WHERE w.session_key IN ({ph}) GROUP BY ALL ORDER BY s.session_name, w.lap_number""", list(session_keys))


def standings_before(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, d.team_name AS team, c.season_starting_position AS season_start_pos,
        c.current_championship_position AS pos, c.total_points AS points
        FROM fct_championship_standings c LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE c.session_key = ? ORDER BY c.current_championship_position""", [session_key])


def constructor_standings(con, session_key: int):
    return q(con, "SELECT team_name AS team, current_championship_position AS pos, total_points AS points "
                  "FROM fct_constructor_standings WHERE session_key = ? ORDER BY current_championship_position", [session_key])


# ---------------------------------------------------------------------------------------------------------------------
# Timing, telemetry and events (used by all three roles)
# ---------------------------------------------------------------------------------------------------------------------
def lap_table(con, session_key: int):
    """One row per driver per lap with timing, gaps, tyre and track state (the analyst's central table)."""
    return q(con, """SELECT d.driver_acronym AS driver, d.team_name AS team, l.driver_number, l.lap_number, l.lap_time_seconds,
        l.duration_sector_1 AS s1, l.duration_sector_2 AS s2, l.duration_sector_3 AS s3, l.speed_trap_finish,
        l.tyre_compound, l.tyre_age_this_lap, l.stint_number, l.track_status, l.is_in_lap, l.is_out_lap, l.is_representative_lap,
        l.fuel_corrected_lap_time_seconds, l.delta_to_driver_median_seconds, l.delta_to_session_best_representative_seconds,
        l.position_end, l.gap_to_leader_end_seconds, l.gap_to_car_ahead_end_seconds, l.lap_start_ts
        FROM fct_lap_performance l LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE l.session_key = ? ORDER BY l.lap_number, l.position_end""", [session_key])


def telemetry_lap_summary(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, t.driver_number, t.lap_number, t.top_speed_kph, t.min_speed_kph, t.avg_speed_kph,
        t.full_throttle_fraction, t.braking_fraction, t.braking_zone_count, t.drs_open_fraction, t.max_gear
        FROM fct_telemetry_lap_summary t LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE t.session_key = ? ORDER BY t.lap_number, d.driver_acronym""", [session_key])


def fastest_laps(con, session_key: int):
    return q(con, """SELECT d.driver_acronym AS driver, f.driver_number, f.lap_number, f.fastest_lap_time_seconds, f.tyre_compound,
        f.speed_trap_finish, f.theoretical_best_lap_seconds, f.time_left_on_table_seconds, f.session_rank, f.gap_to_session_fastest_seconds
        FROM fct_fastest_laps f LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE f.session_key = ? ORDER BY f.session_rank""", [session_key])


def distance_trace(con, session_key: int, driver_number: int, lap_number: int):
    return q(con, """SELECT distance_m, avg_speed_kph AS speed_kph, avg_throttle AS throttle, max_brake AS brake, gear, x, y, elapsed_seconds, is_complete_lap
        FROM fct_telemetry_distance_trace WHERE session_key = ? AND driver_number = ? AND lap_number = ? ORDER BY distance_bin""",
             [session_key, driver_number, lap_number])


def overtakes(con, session_key: int):
    return q(con, """SELECT o.lap_number, a.driver_acronym AS overtaker, b.driver_acronym AS overtaken, o.post_overtake_position AS new_pos,
        o.was_in_drs_range_before_pass AS drs_range_proxy, o.is_probable_pit_cycle_pass AS pit_cycle, o.overtaker_tyre_compound, o.tyre_age_delta
        FROM fct_overtakes o
        LEFT JOIN dim_driver_session a ON a.session_key = o.session_key AND a.driver_number = o.overtaking_driver_number
        LEFT JOIN dim_driver_session b ON b.session_key = o.session_key AND b.driver_number = o.overtaken_driver_number
        WHERE o.session_key = ? ORDER BY o.lap_number""", [session_key])


def penalties(con, session_key: int):
    return q(con, """SELECT p.lap_number, d.driver_acronym AS driver, p.penalty_type, p.penalty_seconds, p.reason, p.served_lap
        FROM fct_penalties p LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE p.session_key = ? ORDER BY p.lap_number""", [session_key])


def neutralisations(con, session_key: int):
    return q(con, "SELECT track_status, start_lap, end_lap, duration_laps FROM fct_safety_car_events WHERE session_key = ? ORDER BY start_lap", [session_key])


def lap_incidents(con, session_key: int):
    return q(con, """SELECT i.lap_number, d.driver_acronym AS driver, i.neutralisation_status, i.primary_flag, i.incident_categories,
        i.race_control_message_count AS messages
        FROM fct_lap_incidents i LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE i.session_key = ? AND i.is_affected_by_incident ORDER BY i.lap_number, d.driver_acronym""", [session_key])


def positions_by_lap(con, session_key: int):
    return q(con, """SELECT t.lap_number, d.driver_acronym AS driver, t.position_by_cumulative_time AS position
        FROM fct_race_trace t LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE t.session_key = ? ORDER BY t.lap_number, t.position_by_cumulative_time""", [session_key])


def pit_loss(con, circuit_key: int):
    return q(con, "SELECT * FROM fct_pit_loss_by_circuit WHERE circuit_key = ?", [circuit_key])


def data_coverage(con):
    """Row counts of the marts the dashboard reads, to show what the warehouse does and does not contain."""
    tables = ["dim_sessions", "dim_driver_session", "fct_lap_performance", "fct_telemetry_lap_summary", "fct_telemetry_distance_trace",
              "fct_race_results", "fct_race_trace", "fct_stints", "fct_pit_stops", "fct_overtakes", "fct_penalties", "fct_weather_impact",
              "fct_safety_car_events", "fct_compound_pace_model", "fct_radio_messages", "dim_session_corners"]
    rows = []
    for t in tables:
        try:
            rows.append({"table": t, "rows": int(q(con, f"SELECT COUNT(*) FROM {t}").iloc[0, 0])})
        except Exception:
            rows.append({"table": t, "rows": None})
    return pd.DataFrame(rows)


def points_before(con, year: int, before_ts):
    """Championship points per driver computed from results of sessions that started BEFORE `before_ts` (leakage-safe standings)."""
    return q(con, """SELECT d.driver_acronym AS driver, d.team_name AS team, p.cumulative_points AS points, p.championship_position_after_session AS pos
        FROM fct_points_progression p LEFT JOIN dim_driver_session d USING (session_key, driver_number)
        WHERE p.year = ? AND p.session_start_time < ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.driver_number ORDER BY p.session_start_time DESC) = 1
        ORDER BY pos""", [year, before_ts])


def sc_events_at_circuit(con, circuit_key: int, before_ts):
    return q(con, """SELECT e.year, e.track_status, e.start_lap, e.end_lap, e.duration_laps FROM fct_safety_car_events e JOIN dim_sessions s USING (session_key)
        WHERE e.circuit_key = ? AND s.session_start_time < ? ORDER BY s.session_start_time DESC, e.start_lap""", [circuit_key, before_ts])


def race_order(con, session_key: int):
    """Position and gaps from CUMULATIVE race time: internally consistent (no duplicate positions), unlike the position feed sampled at lap end."""
    return q(con, """SELECT t.lap_number, t.driver_number, t.position_by_cumulative_time AS position,
        t.gap_to_leader_by_cumulative_time_seconds AS gap_to_leader,
        t.gap_to_leader_by_cumulative_time_seconds - LAG(t.gap_to_leader_by_cumulative_time_seconds) OVER (PARTITION BY t.lap_number ORDER BY t.position_by_cumulative_time) AS interval
        FROM fct_race_trace t WHERE t.session_key = ? ORDER BY t.lap_number, t.position_by_cumulative_time""", [session_key])


def is_fixture_warehouse(con) -> bool:
    """True for the synthetic CI warehouse (built by `make ci-test`): by file name, or by holding only the fixture session keys 9001-9003."""
    name = (CURRENT_DB or "").replace("\\", "/").lower()
    if "ci.duckdb" in name or ".ci_raw" in name:
        return True
    keys = set(q(con, "SELECT session_key FROM dim_sessions")["session_key"])
    return bool(keys) and keys <= {9001, 9002, 9003, 9004}


def incident_summary(con, session_key: int):
    """One row per lap and kind of incident (not one per driver per lap): a yellow flag over the whole field is one event."""
    return q(con, """SELECT lap_number, neutralisation_status, primary_flag, incident_categories,
        MAX(race_control_message_count) AS messages, COUNT(*) AS cars_affected
        FROM fct_lap_incidents WHERE session_key = ? AND is_affected_by_incident
        GROUP BY lap_number, neutralisation_status, primary_flag, incident_categories ORDER BY lap_number, primary_flag""", [session_key])


def health_checks(con):
    """Data-health table: things that were wrong on real data and that a pipeline run should keep visible."""
    def one(sql):
        try:
            v = q(con, sql).iloc[0, 0]
            return None if pd.isna(v) else v          # an empty aggregate (for example no VSC at all) is None, not pd.NA
        except Exception:
            return None
    rows = []
    races = one("SELECT COUNT(DISTINCT session_key) FROM fct_race_results WHERE session_type IN ('Race','Sprint')")
    with_grid = one("SELECT COUNT(*) FROM (SELECT session_key FROM fct_race_results WHERE session_type IN ('Race','Sprint') GROUP BY 1 HAVING COUNT(grid_position) > 0) t")
    rows.append(("Races/sprints with starting-grid positions", f"{with_grid} of {races}", None if races is None else ("ok" if with_grid == races else "warn"),
                 "Without them: no positions gained/lost and no grid-vs-finish baseline. Check the starting_grid extract and its session_key."))
    tel = one("SELECT COUNT(DISTINCT t.session_key) FROM fct_telemetry_lap_summary t JOIN dim_sessions s USING (session_key) WHERE s.session_type IN ('Race','Sprint')")
    rows.append(("Races/sprints with telemetry", f"{tel} of {races}", "ok" if tel and tel == races else "info",
                 "Telemetry is large and loaded per session; build more with telemetry_only_sessions."))
    vsc = one("SELECT MAX(duration_laps) FROM fct_safety_car_events WHERE track_status = 'VSC'")
    sc = one("SELECT MAX(duration_laps) FROM fct_safety_car_events WHERE track_status = 'SC'")
    rows.append(("Longest VSC / SC period (laps)", f"{vsc} / {sc}", None if vsc is None and sc is None else ("warn" if (vsc or 0) > 12 or (sc or 0) > 20 else "ok"),
                 "A VSC far longer than ~12 laps means a closing message was missed and nothing superseded it; unclosed periods are capped."))
    pit = one("SELECT ROUND(MEDIAN(pit_lane_time_seconds), 1) FROM fct_pit_stops")
    rows.append(("Median pit-lane time, race stops (s)", str(pit), None if pit is None else ("ok" if 15 <= pit <= 40 else "warn"),
                 "Practice/qualifying garage visits are excluded; a value outside 15-40 s points at a units or data problem."))
    nulls = one("""SELECT ROUND(100.0 * AVG(CASE WHEN l.lap_time_seconds IS NULL THEN 1 ELSE 0 END), 1) FROM fct_lap_performance l
                   JOIN dim_sessions s USING (session_key) WHERE s.session_type IN ('Race','Sprint')""")
    rows.append(("Race laps without a lap time (%)", str(nulls), None if nulls is None else ("ok" if nulls < 5 else "warn"),
                 "Mostly retiring cars' last laps. Cumulative race time falls back to lap-start differences."))
    return pd.DataFrame(rows, columns=["check", "result", "status", "why it matters"])


def drivers_with_laps(con, session_key: int):
    """Driver numbers that have at least one lap row in the session (a DNS driver has none)."""
    return set(q(con, "SELECT DISTINCT driver_number FROM fct_lap_performance WHERE session_key = ?", [session_key])["driver_number"])


def race_events(con, session_key: int):
    """One table of everything that happened, by lap: neutralisations, penalties, pit stops, retirements and the fastest lap."""
    parts = []
    nz = neutralisations(con, session_key)
    if not nz.empty:
        parts.append(pd.DataFrame({"lap": nz["start_lap"], "end_lap": nz["end_lap"], "kind": "Neutralisation", "driver": None,
                                   "detail": nz["track_status"] + " for " + nz["duration_laps"].astype(str) + " laps (" + nz["start_lap"].astype(str) + "-" + nz["end_lap"].astype(str) + ")"}))
    pn = penalties(con, session_key)
    if not pn.empty:
        parts.append(pd.DataFrame({"lap": pn["lap_number"], "kind": "Penalty", "driver": pn["driver"],
                                   "detail": pn["penalty_type"].astype(str) + " " + pn["penalty_seconds"].astype(str) + "s: " + pn["reason"].astype(str)
                                             + pn["served_lap"].map(lambda v: "" if pd.isna(v) else f" (served lap {int(v)})")}))
    ps = pit_stops(con, session_key)
    if not ps.empty:
        under = ps["is_under_neutralisation"].fillna(False).map({True: " (under neutralisation)", False: ""})
        parts.append(pd.DataFrame({"lap": ps["lap_number"], "kind": "Pit stop", "driver": ps["driver"],
                                   "detail": ps["compound_before"].astype(str) + " > " + ps["compound_after"].astype(str) + ", pit lane " + ps["pit_lane_time_seconds"].round(1).astype(str) + "s" + under}))
    rs = results(con, session_key)
    if not rs.empty:
        dnf = rs[rs["status"].isin(["DNF", "DSQ"]) & (rs["laps"] > 0)]
        if not dnf.empty:
            parts.append(pd.DataFrame({"lap": dnf["laps"] + 1, "kind": "Retirement", "driver": dnf["driver"], "detail": dnf["status"] + " after " + dnf["laps"].astype(str) + " laps"}))
    fl = fastest_laps(con, session_key)
    if not fl.empty:
        top = fl.iloc[0]
        parts.append(pd.DataFrame({"lap": [top["lap_number"]], "kind": "Fastest lap", "driver": [top["driver"]], "detail": [f"{top['fastest_lap_time_seconds']:.3f}s"]}))
    if not parts:
        return pd.DataFrame(columns=["lap", "end_lap", "kind", "driver", "detail"])
    ev = pd.concat(parts, ignore_index=True).dropna(subset=["lap"])
    ev["lap"] = ev["lap"].astype(int)
    return ev.sort_values(["lap", "kind"]).reset_index(drop=True)


def penalties_so_far(pn: pd.DataFrame, lap: int) -> pd.DataFrame:
    """Penalties issued up to `lap` for a replay. A penalty's `served_lap` is only shown once that lap has been reached: before, it is future information."""
    d = pn[pn["lap_number"] <= lap].copy()
    if "served_lap" in d:
        d["served_lap"] = d["served_lap"].where(d["served_lap"].isna() | (d["served_lap"] <= lap)).astype("Int64")
    return d


def sessions_with_telemetry(con):
    """Session keys that have per-lap telemetry built (telemetry is large and is built per session with telemetry_only_sessions)."""
    return set(q(con, "SELECT DISTINCT session_key FROM fct_telemetry_lap_summary")["session_key"])


def race_distance(con, session_key: int) -> int:
    """Race distance in laps = laps completed by the classified leader. NOT MAX(lap_number) of the lap table: that also counts rows recorded after the
    flag (cool-down laps), which made a 56-lap race look like 58. Falls back to the lap table when there is no classification."""
    df = q(con, "SELECT MAX(laps_completed) FROM fct_race_results WHERE session_key = ? AND classification_status IN ('Finished', 'Lapped')", [session_key])
    if not df.empty and pd.notna(df.iloc[0, 0]) and int(df.iloc[0, 0]) > 0:
        return int(df.iloc[0, 0])
    return session_laps(con, session_key)
