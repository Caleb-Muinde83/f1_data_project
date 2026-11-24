import pathlib
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest
from f1_dash import data, charts, insights
from f1_dash.components import timing

RACE, RACE_MEETING = 9002, 1001       # synthetic fixtures: 9001 qualifying + 9002 race in meeting 1001, 9003 race in meeting 1002
APP = str(pathlib.Path(__file__).resolve().parents[1] / "app.py")


@pytest.fixture
def con(warehouse_path):
    c = data.connect(warehouse_path)
    yield c
    c.close()


def test_data_layer_returns_expected_shapes(con):
    assert not data.sessions(con).empty and set(data.sessions(con, race_only=True).session_type) <= {"Race", "Sprint"}
    res = data.results(con, RACE)
    assert list(res["pos"]) == sorted(res["pos"]) and res["points"].sum() > 0
    assert not data.race_trace(con, RACE).empty and not data.stints(con, RACE).empty
    assert data.pit_stops(con, RACE)["pit_lane_time_seconds"].gt(0).all()
    assert set(data.undercuts(con, RACE).strategy_type) <= {"UNDERCUT", "OVERCUT", "SAME_LAP"}
    c = data.corners(con, RACE)
    assert c.corner_number.nunique() == 3
    assert not data.sc_hazard(con).empty and not data.radio(con, RACE).empty


def test_new_queries_have_the_columns_the_workspaces_rely_on(con):
    assert {"driver", "lap_number", "position_end", "gap_to_car_ahead_end_seconds", "stint_number"} <= set(data.lap_table(con, RACE).columns)
    assert not data.telemetry_lap_summary(con, RACE).empty and not data.fastest_laps(con, RACE).empty
    assert not data.meetings(con).empty and data.session_laps(con, RACE) > 0
    tr = data.distance_trace(con, RACE, int(data.drivers(con, RACE).driver_number.iloc[0]), int(data.fastest_laps(con, RACE).lap_number.iloc[0]))
    assert {"distance_m", "speed_kph", "x", "y"} <= set(tr.columns)
    assert not data.data_coverage(con).empty


def test_pre_race_queries_never_see_the_race_itself(con):
    """Leakage guard: everything a pre-race page reads must pre-date the target race."""
    race = data.session_row(con, RACE)
    cutoff = race.session_start_time
    assert RACE not in set(data.prior_sessions(con, int(race.meeting_key), cutoff).session_key)
    assert RACE not in set(data.grid_finish_history(con, cutoff).session_key)
    assert RACE not in set(data.prior_races_at_circuit(con, int(race.circuit_key), cutoff).session_key)
    pts = data.points_before(con, int(race.year), cutoff)
    full = data.points_progression(con, int(race.year))
    assert pts.points.sum() <= full.groupby("driver").cumulative_points.max().sum()


def test_season_aware_points_scored_the_race_with_that_seasons_scale(con):
    got = data.q(con, "SELECT SUM(position_points) FROM fct_race_results WHERE session_key = ?", [RACE]).iloc[0, 0]
    expected = data.q(con, """SELECT SUM(p.points) FROM fct_race_results r JOIN dim_sessions s USING (session_key)
        JOIN f1_points_system p ON p.season = s.year AND p.session_type = s.session_type AND p.position = r.final_position
        WHERE r.session_key = ?""", [RACE]).iloc[0, 0]
    assert got == expected and got > 0


def test_connect_errors_are_actionable(tmp_path):
    with pytest.raises(FileNotFoundError, match="F1_DUCKDB_PATH"):
        data.connect(str(tmp_path / "missing.duckdb"))


def test_charts_return_figures_and_handle_empty_input(con):
    figs = [charts.race_trace_fig(data.race_trace(con, RACE)), charts.lap_scatter_fig(data.lap_pace(con, RACE)),
            charts.stint_gantt_fig(data.stints(con, RACE), data.pit_stops(con, RACE)), charts.degradation_fig(data.stints(con, RACE)),
            charts.delta_trace_fig(data.delta_trace(con, RACE)), charts.corner_fig(data.corners(con, RACE)),
            charts.sc_hazard_fig(data.sc_hazard(con)), charts.points_fig(data.points_progression(con, 2024)),
            charts.position_progression_fig(data.positions_by_lap(con, RACE)), charts.sector_heatmap_fig(data.lap_table(con, RACE)),
            charts.pit_duration_fig(data.pit_stops(con, RACE)), charts.pace_rolling_fig(data.lap_table(con, RACE), list(data.lap_table(con, RACE).driver.unique()[:2]))]
    assert all(len(f.data) > 0 for f in figs)
    empties = [charts.race_trace_fig(pd.DataFrame()), charts.stint_gantt_fig(pd.DataFrame()), charts.delta_trace_fig(pd.DataFrame()),
               charts.sc_hazard_fig(pd.DataFrame()), charts.strategy_fig([])]
    assert all(len(f.layout.annotations) == 1 for f in empties)      # a message, not a crash


def test_stint_gantt_bars_cover_the_right_laps(con):
    fig = charts.stint_gantt_fig(data.stints(con, RACE))
    total_laps = sum(float(sum(t.x)) for t in fig.data)
    expected = data.stints(con, RACE).eval("last_lap - first_lap + 1").sum()
    assert total_laps == expected


def test_timing_tower_orders_by_position_and_is_replay_safe(con):
    laps = data.lap_table(con, RACE)
    t = timing.tower(laps, 3)
    assert list(t["Pos"]) == sorted(t["Pos"]) and len(t) == laps[laps.lap_number == 3].shape[0]
    fit = insights.fit_stint_degradation(laps, int(laps.driver_number.iloc[0]), upto_lap=4, min_points=2)
    assert fit is None or max(fit["lap"]) <= 4


WORKSPACES = ["engineer.pre_race", "engineer.during_race", "engineer.post_race", "analyst.pre_race", "analyst.during_race", "analyst.post_race",
              "scientist.pre_race", "scientist.during_race", "scientist.post_race", "platform"]


def _page(module, db, session=RACE, meeting=RACE_MEETING, lap=None, lap_key=None):
    def script(module, db, session, meeting):
        import importlib
        import streamlit as st
        from f1_dash import data
        st.session_state["ctx_meeting"], st.session_state["ctx_session"] = meeting, session
        importlib.import_module(f"f1_dash.workspaces.{module}").render(data.connect(db))
    at = AppTest.from_function(script, args=(module, db, session, meeting), default_timeout=90)
    at.run()
    return at


@pytest.mark.parametrize("module", WORKSPACES)
def test_every_workspace_page_renders_without_error(module, warehouse_path):
    at = _page(module, warehouse_path)
    assert not at.exception, [str(e.value) for e in at.exception]
    assert len(at.header) >= 1 and len(at.get("plotly_chart")) + len(at.dataframe) > 0


@pytest.mark.parametrize("session,meeting", [(9001, 1001), (9003, 1002)])
@pytest.mark.parametrize("module", WORKSPACES[:-1])
def test_workspaces_survive_other_sessions_including_a_qualifying_session(module, session, meeting, warehouse_path):
    at = _page(module, warehouse_path, session, meeting)
    assert not at.exception, [str(e.value) for e in at.exception]


@pytest.mark.parametrize("module", ["engineer.during_race", "analyst.during_race", "scientist.during_race"])
def test_replay_pages_are_labelled_replay_and_follow_the_lap_slider(module, warehouse_path):
    at = _page(module, warehouse_path)
    assert any("REPLAY" in m.value for m in at.markdown) and any("NOT a live feed" in m.value for m in at.markdown)
    key = {"engineer": "eng_dur_lap", "analyst": "ana_dur_lap", "scientist": "ds_dur_lap"}[module.split(".")[0]]
    at.slider(key=key).set_value(2).run()
    assert not at.exception, [str(e.value) for e in at.exception]


def test_pre_race_pages_state_their_information_cutoff(warehouse_path):
    for module in ("engineer.pre_race", "analyst.pre_race", "scientist.pre_race"):
        at = _page(module, warehouse_path)
        assert any("cut-off" in c.value for c in at.caption), module


def test_scientist_pre_race_warns_about_simulator_leakage_and_post_race_labels_in_sample(warehouse_path):
    pre = _page("scientist.pre_race", warehouse_path)
    post = _page("scientist.post_race", warehouse_path)
    assert any("EVERY race" in m.value for m in pre.markdown)
    assert any("In-sample" in m.value for m in post.markdown)


def test_engineer_post_race_has_the_telemetry_overlay_and_circuit_map(warehouse_path):
    at = _page("engineer.post_race", warehouse_path)
    assert len(at.get("plotly_chart")) >= 5 and len(at.tabs) == 4


def test_full_app_boots_with_the_shared_sidebar_context(warehouse_path, monkeypatch):
    monkeypatch.setenv("F1_DUCKDB_PATH", warehouse_path)
    at = AppTest.from_file(APP, default_timeout=120).run()
    assert not at.exception, [str(e.value) for e in at.exception]
    assert [s.label for s in at.sidebar.selectbox][:3] == ["Season", "Grand Prix", "Session"]
    assert at.header[0].value == "Race Engineer: Pre-race"


def test_missing_warehouse_shows_a_message_not_a_stack_trace(tmp_path):
    def script(db):
        import streamlit as st
        from f1_dash import data
        try:
            data.connect(db)
        except Exception as exc:
            st.error(str(exc))
    at = AppTest.from_function(script, args=(str(tmp_path / "nope.duckdb"),)); at.run()
    assert not at.exception and "Warehouse not found" in at.error[0].value


def test_race_order_positions_are_unique_per_lap(con):
    o = data.race_order(con, RACE)
    assert not o.groupby("lap_number").position.apply(lambda s: s.duplicated().any()).any()


def test_query_results_carry_no_float_noise(con):
    fl = data.fastest_laps(con, RACE)
    assert (fl.time_left_on_table_seconds.abs().dropna().eq(0) | (fl.time_left_on_table_seconds.abs().dropna() >= 1e-6)).all()


def test_fixture_warehouse_is_flagged_in_the_sidebar(warehouse_path, monkeypatch):
    c = data.connect(warehouse_path)
    assert data.is_fixture_warehouse(c) and data.CURRENT_DB == warehouse_path
    monkeypatch.setenv("F1_DUCKDB_PATH", warehouse_path)
    at = AppTest.from_file(APP, default_timeout=120).run()
    assert any("SYNTHETIC TEST DATA" in w.value for w in at.sidebar.warning)


def test_identical_charts_on_one_page_do_not_collide():
    """Regression: two identical 'no data' placeholders raised StreamlitDuplicateElementId on real data."""
    def script():
        import streamlit as st
        from f1_dash import charts
        from f1_dash.components import plot
        plot.reset()
        plot.show(charts.lap_summary_fig(__import__("pandas").DataFrame(), "x", "x"))
        plot.show(charts.lap_summary_fig(__import__("pandas").DataFrame(), "x", "x"))
        c1, c2 = st.columns(2)
        plot.show(charts.circuit_map_fig(__import__("pandas").DataFrame()), c1)
        plot.show(charts.circuit_map_fig(__import__("pandas").DataFrame()), c2)
    at = AppTest.from_function(script).run()
    assert not at.exception, [str(e.value) for e in at.exception]
    assert len(at.get("plotly_chart")) == 4


@pytest.mark.parametrize("module", ["engineer.pre_race", "engineer.post_race", "engineer.during_race", "scientist.during_race"])
def test_pages_survive_a_session_with_no_telemetry(module, warehouse_path):
    """Real sessions often lack telemetry (it is large and loaded per session): every telemetry panel must degrade to a message."""
    def script(module, db):
        import importlib
        import pandas as pd
        import streamlit as st
        from f1_dash import data
        st.session_state["ctx_meeting"], st.session_state["ctx_session"] = 1001, 9002
        data.telemetry_lap_summary = lambda con, sk: pd.DataFrame()
        data.distance_trace = lambda con, sk, d, lap: pd.DataFrame()
        data.fastest_laps = lambda con, sk: pd.DataFrame(columns=["driver_number", "lap_number"])
        importlib.import_module(f"f1_dash.workspaces.{module}").render(data.connect(db))
    at = AppTest.from_function(script, args=(module, warehouse_path), default_timeout=90).run()
    assert not at.exception, [str(e.value) for e in at.exception]


def test_cursors_allow_overlapping_reads_from_many_threads(con):
    """Regression: a shared DuckDB connection raised 'connection is busy' when Streamlit script runs overlapped."""
    from concurrent.futures import ThreadPoolExecutor
    def work(_):
        cur = con.cursor()
        return len(data.meetings(cur)) + len(data.lap_table(cur, RACE))
    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(work, range(40)))
    assert len(set(results)) == 1


def test_garage_visits_never_pollute_pit_stops_or_circuit_pit_loss(con):
    """Regression (found on real data): practice/qualifying 'pit stops' are garage visits with minutes of pit-lane time."""
    assert data.q(con, "SELECT COUNT(*) FROM stg_pit p JOIN dim_sessions s USING (session_key) WHERE s.session_type NOT IN ('Race','Sprint')").iloc[0, 0] > 0
    assert data.q(con, "SELECT COUNT(*) FROM fct_pit_stops p JOIN dim_sessions s USING (session_key) WHERE s.session_type NOT IN ('Race','Sprint')").iloc[0, 0] == 0
    pl = data.pit_loss(con, int(data.session_row(con, RACE).circuit_key))
    assert 10 < float(pl.median_green_pit_lane_time_seconds.iloc[0]) < 120


def test_grid_positions_resolve_directly_and_via_the_qualifying_session(con):
    """Real OpenF1 keys starting_grid rows to the session that set the grid. Race 9002's rows are keyed to Qualifying 9001; 9003 is keyed directly."""
    src = data.q(con, "SELECT session_key, grid_source, COUNT(grid_position) AS n FROM fct_race_results WHERE session_type IN ('Race', 'Sprint') GROUP BY 1, 2 ORDER BY 1")
    # race 9002: grid keyed to Qualifying 9001. Sprint 9003 (OpenF1 type 'Race', name 'Sprint'): keyed to Sprint Qualifying 9004. Qualifying runs AFTER a sprint,
    # so a Sprint must never be matched to the weekend's Qualifying session.
    # Sprint 9006 (meeting 1003) has NO starting_grid rows at all, as the real 2025 Brazilian Sprint: its grid is the Sprint Qualifying classification, never mixed with the feed.
    assert dict(zip(src.session_key, src.grid_source)) == {9002: "starting_grid_via_qualifying", 9003: "starting_grid_via_qualifying", 9006: "qualifying_classification"} and (src.n > 0).all()
    assert not data.starting_grid(con, RACE).empty


def test_a_retired_cars_unfinished_lap_does_not_distort_the_race_trace(con):
    """Real data: a retiring driver's last lap has no duration; SUM skipped it and everyone looked 140 s behind at that lap."""
    o = data.race_order(con, 9003)
    assert not o.empty and o.gap_to_leader.max() < 60
    retired = o[o.driver_number == 44]
    assert retired.lap_number.max() <= 8                     # no cumulative time for his unfinished lap 9


def test_incident_summary_is_one_row_per_event_not_per_car(con):
    per_car, summary = data.lap_incidents(con, RACE), data.incident_summary(con, RACE)
    assert len(summary) < len(per_car) and summary.cars_affected.max() > 1


def test_data_health_checks_run_and_flag_nothing_on_the_fixtures(con):
    h = data.health_checks(con)
    assert len(h) == 5 and not (h.status == "warn").any(), h.to_dict("records")
    assert h.set_index("check").loc["Races/sprints with starting-grid positions", "result"] == "3 of 3"


def test_telemetry_after_the_last_lap_is_not_credited_to_it(con):
    """Regression: samples recorded after a lap ended (cool-down, garage) used to be credited to the previous lap."""
    late = data.q(con, """SELECT COUNT(*) FROM fct_telemetry t JOIN stg_laps l USING (session_key, driver_number, lap_number)
        WHERE EXTRACT(EPOCH FROM (t.telemetry_timestamp - l.date_start)) > l.lap_duration + 5""").iloc[0, 0]
    assert late == 0 and data.q(con, "SELECT COUNT(*) FROM fct_telemetry WHERE lap_number IS NULL").iloc[0, 0] > 0


def test_a_sprint_is_typed_sprint_and_scored_on_the_sprint_scale(con):
    """Regression (found on real data): OpenF1 types a Sprint 'Race', so sprints were scored 25-18-15... and could not find their grid."""
    row = data.session_row(con, 9003)
    assert row.session_name == "Sprint" and row.session_type == "Sprint"
    pts = data.q(con, "SELECT MAX(total_points) FROM fct_race_results WHERE session_key = 9003").iloc[0, 0]
    assert pts == 8
    assert data.q(con, "SELECT MAX(total_points) FROM fct_race_results WHERE session_key = 9002").iloc[0, 0] >= 25


def test_a_driver_with_no_lap_one_data_is_not_ahead_of_the_field(con):
    """Regression (found on real data): with no lap-1 duration/start SUM omitted lap 1, so that driver looked ~100 s ahead for the whole race."""
    o = data.race_order(con, 9003)
    d63 = o[o.driver_number == 63].set_index("lap_number")
    leader_gap = d63.gap_to_leader
    assert leader_gap.loc[2] >= 0 and leader_gap.max() < 60 and (o.groupby("lap_number").position.max() >= 3).all()
    assert o[o.lap_number == 1].shape[0] >= 3            # lap 1 is still ranked: ends come from the next lap's start


def test_a_vsc_upgraded_to_a_safety_car_ends_when_the_safety_car_starts(con):
    """Regression (real 2024 Shanghai): VSC deployed, then SAFETY CAR DEPLOYED 4 minutes later, and no VSC ENDING message at all."""
    p = data.q(con, "SELECT track_status, period_start, period_end, is_unclosed FROM int_track_status_periods WHERE session_key = 9003 ORDER BY period_start")
    first, sc = p.iloc[0], p.iloc[1]
    assert (first.track_status, sc.track_status) == ("VSC", "SC")
    assert first.period_end == sc.period_start and not first.is_unclosed        # ends exactly when the SC starts
    assert p[p.track_status == "VSC"].is_unclosed.tolist() == [False, True]       # the later VSC really has no ending and stays capped


def test_a_session_without_laps_does_not_crash_session_laps(con):
    """Regression (crashed the real app): MAX(lap_number) of a session with no laps is NA, and `NA == NA` is ambiguous."""
    assert data.session_laps(con, 9004) == 0 and data.drivers_with_laps(con, 9004) == set()
    assert data.session_laps(con, RACE) == 12 and 1 in data.drivers_with_laps(con, RACE)


def test_race_events_put_everything_on_one_lap_axis(con):
    ev = data.race_events(con, RACE)
    assert {"Neutralisation", "Pit stop", "Fastest lap"} <= set(ev.kind) and ev.lap.is_monotonic_increasing and ev.lap.between(1, 13).all()
    sprint = data.race_events(con, 9003)
    assert "Retirement" in set(sprint.kind) and set(sprint[sprint.kind == "Retirement"].driver) == {"HAM"}


def _page_with(module, db, patch="", session=RACE, meeting=RACE_MEETING, state=""):
    def script(module, db, session, meeting, patch, state):
        import importlib
        import pandas as pd
        import streamlit as st
        from f1_dash import data
        st.session_state["ctx_meeting"], st.session_state["ctx_session"] = meeting, session
        ns = {"data": data, "pd": pd, "st": st}
        exec(state, ns)
        exec(patch, ns)
        importlib.import_module(f"f1_dash.workspaces.{module}").render(data.connect(db))
    return AppTest.from_function(script, args=(module, db, session, meeting, patch, state), default_timeout=90).run()


def test_a_focus_driver_without_laps_is_called_out_not_silently_blank(warehouse_path):
    at = _page_with("engineer.during_race", warehouse_path, patch="data.drivers_with_laps = lambda con, sk: {1, 11}", state="st.session_state['ctx_driver'] = 63")
    assert not at.exception, [str(e.value) for e in at.exception]
    assert any("no laps in this session" in w.value for w in at.warning)


def test_jump_to_event_moves_the_replay_lap(warehouse_path):
    at = _page_with("analyst.during_race", warehouse_path)
    options = at.selectbox(key="ana_dur_lap_jump").options
    assert len(options) > 1
    target = options[1]
    at.selectbox(key="ana_dur_lap_jump").select(target).run()
    assert not at.exception, [str(e.value) for e in at.exception]
    assert at.slider(key="ana_dur_lap").value == int(target.split(":")[0].replace("Lap ", ""))


@pytest.mark.parametrize("module", ["engineer.during_race", "analyst.during_race", "scientist.during_race", "analyst.post_race", "scientist.post_race", "engineer.post_race"])
def test_a_race_whose_lap_data_is_missing_renders_instead_of_crashing(module, warehouse_path):
    patch = ("_lt = data.lap_table; data.lap_table = lambda con, sk: _lt(con, sk).head(0)\\n"
             "data.session_laps = lambda con, sk: 0\\n"
             "data.telemetry_lap_summary = lambda con, sk: pd.DataFrame()\\n"
             "data.drivers_with_laps = lambda con, sk: set()")
    at = _page_with(module, warehouse_path, patch=patch.replace("\\n", "\n"))
    assert not at.exception, [str(e.value) for e in at.exception]


def test_simulator_only_offers_sessions_with_two_compounds(con):
    """Regression (real 2025 Shanghai): the page defaulted to a session the simulator then rejected ('found [MEDIUM]')."""
    from f1_dash.components import simulator
    assert simulator.viable(con, [9001, 9002], driver=1, total_laps=12) == [9002]         # qualifying has one compound, the race has two


def test_scientist_pre_race_offers_the_circuits_earlier_races_as_calibration(warehouse_path):
    """When the weekend has no session with two compounds, an earlier race at the same circuit (valid pre-race information) is the calibration source."""
    patch = ("import pandas as pd\n"
             "data.prior_races_at_circuit = lambda con, ck, ts: pd.DataFrame({'year': [2024], 'session_key': [9002], 'session_name': ['Sprint'], "
             "'session_start_time': [pd.Timestamp('2024-03-02')], 'winner': ['VER'], 'winning_team': ['Red Bull Racing']})")
    at = _page_with("scientist.pre_race", warehouse_path, patch=patch, session=9003, meeting=1002)
    assert not at.exception, [str(e.value) for e in at.exception]
    box = at.selectbox(key="ds_pre_calib_9003")
    assert box.options == ["2024 Sprint at this circuit (9002)"] and (box.value == 9002 or "9002" in str(box.value))
    assert at.number_input(key="ds_pre_laps").value == 12            # the earlier Sprint's distance (target is a Sprint), not a hard-coded 57
    assert not any("cannot be calibrated" in c.value for c in at.caption)


def test_scientist_pre_race_says_so_when_nothing_can_calibrate_the_simulator(warehouse_path):
    at = _page_with("scientist.pre_race", warehouse_path, session=9003, meeting=1002)
    assert not at.exception, [str(e.value) for e in at.exception]
    assert any("cannot be calibrated" in c.value for c in at.caption)


def test_points_progression_defaults_to_the_selected_race(warehouse_path):
    at = _page_with("analyst.post_race", warehouse_path)
    assert not at.exception, [str(e.value) for e in at.exception]
    assert at.checkbox(key="ana_post_season").value is False


def test_a_penalty_served_notice_is_not_a_second_penalty(con):
    """Regression (real 2026 Shanghai): 'FIA STEWARDS: PENALTY SERVED - 10 SECOND TIME PENALTY FOR CAR 31 (OCO) - ...' was counted as a second penalty."""
    pn = data.penalties(con, RACE)
    assert len(pn) == 1 and pn.iloc[0].driver == "PER" and pn.iloc[0].penalty_seconds == 5
    assert pn.iloc[0].lap_number == 9 and pn.iloc[0].served_lap == 11
    ev = data.race_events(con, RACE)
    assert ev[ev.kind == "Penalty"].detail.iloc[0].endswith("(served lap 11)") and (ev.kind == "Penalty").sum() == 1


def test_scientist_race_distance_comes_from_the_last_race_not_a_sprint(warehouse_path):
    """Regression (real 2025 Shanghai): the default distance was the 19-lap Sprint's, so every simulated strategy was 19 laps long."""
    patch = ("import pandas as pd\n"
             "data.prior_races_at_circuit = lambda con, ck, ts: pd.DataFrame({'year': [2025, 2024], 'session_key': [9003, 9002], 'session_name': ['Sprint', 'Race'], "
             "'session_start_time': [pd.Timestamp('2025-03-22'), pd.Timestamp('2024-03-24')], 'winner': ['A', 'B'], 'winning_team': ['X', 'Y']})\n"
             "data.race_distance = lambda con, sk: {9002: 56, 9003: 19}.get(int(sk), 0)")
    at = _page_with("scientist.pre_race", warehouse_path, patch=patch)           # target: Race 9002
    assert not at.exception, [str(e.value) for e in at.exception]
    assert at.number_input(key="ds_pre_laps").value == 56


def test_race_distance_is_the_leaders_laps_not_the_biggest_lap_row(con, monkeypatch):
    """Regression (real 2024 Shanghai): MAX(lap_number) said 58 for a 56-lap race (rows recorded after the flag), so the simulator ran 58-lap strategies."""
    monkeypatch.setattr(data, "session_laps", lambda c, sk: 58)
    assert data.race_distance(con, RACE) == 12                      # the classified leader's laps (fixture race = 12 laps)
    assert data.race_distance(con, 9004) == 58                      # no classification at all -> fall back to the lap table


def test_telemetry_sessions_are_listed(con):
    assert 9001 in data.sessions_with_telemetry(con) and 9004 not in data.sessions_with_telemetry(con)


def test_engineer_pre_race_defaults_to_a_baseline_session_that_has_telemetry(warehouse_path):
    at = _page_with("engineer.pre_race", warehouse_path)
    assert not at.exception, [str(e.value) for e in at.exception]
    assert not any("Telemetry has not been built" in i.value for i in at.info)


def test_engineer_pre_race_says_how_to_build_missing_telemetry(warehouse_path):
    """Real 2026 Shanghai: telemetry existed only for the race, so every pre-race telemetry panel was blank with no explanation."""
    at = _page_with("engineer.pre_race", warehouse_path, patch="data.sessions_with_telemetry = lambda con: set()")
    assert not at.exception, [str(e.value) for e in at.exception]
    msg = " ".join(i.value for i in at.info)
    assert "Telemetry has not been built" in msg and "telemetry_only_sessions: [9001]" in msg
    assert "no telemetry built" in at.selectbox(key="eng_pre_base_9002").options[0]
