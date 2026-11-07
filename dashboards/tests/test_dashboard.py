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
