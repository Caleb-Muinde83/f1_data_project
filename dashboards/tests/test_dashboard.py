import pytest
from streamlit.testing.v1 import AppTest
from f1_dash import data, charts

RACE = 9002


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


def test_connect_errors_are_actionable(tmp_path):
    with pytest.raises(FileNotFoundError, match="F1_DUCKDB_PATH"):
        data.connect(str(tmp_path / "missing.duckdb"))


def test_charts_return_figures_and_handle_empty_input(con):
    import pandas as pd
    figs = [charts.race_trace_fig(data.race_trace(con, RACE)), charts.lap_scatter_fig(data.lap_pace(con, RACE)),
            charts.stint_gantt_fig(data.stints(con, RACE)), charts.degradation_fig(data.stints(con, RACE)),
            charts.delta_trace_fig(data.delta_trace(con, RACE)), charts.corner_fig(data.corners(con, RACE)),
            charts.sc_hazard_fig(data.sc_hazard(con)),
            charts.points_fig(data.points_progression(con, 2024))]
    assert all(len(f.data) > 0 for f in figs)
    empties = [charts.race_trace_fig(pd.DataFrame()), charts.stint_gantt_fig(pd.DataFrame()), charts.delta_trace_fig(pd.DataFrame()),
               charts.sc_hazard_fig(pd.DataFrame()), charts.strategy_fig([])]
    assert all(len(f.layout.annotations) == 1 for f in empties)      # a message, not a crash


def test_stint_gantt_bars_cover_the_right_laps(con):
    fig = charts.stint_gantt_fig(data.stints(con, RACE))
    total_laps = sum(float(sum(t.x)) for t in fig.data)
    expected = data.stints(con, RACE).eval("last_lap - first_lap + 1").sum()
    assert total_laps == expected


def _page(name, path):
    def script(page_name, db):
        from f1_dash import data, pages
        con = data.connect(db)
        getattr(pages, page_name)(con)
    at = AppTest.from_function(script, args=(name, path), default_timeout=90)
    at.run()
    return at


@pytest.mark.parametrize("page", ["overview", "pace", "strategy", "telemetry", "safety_car", "radio"])
def test_every_page_renders_without_error(page, warehouse_path):
    at = _page(page, warehouse_path)
    assert not at.exception, [str(e.value) for e in at.exception]
    assert len(at.header) == 1 and len(at.get("plotly_chart")) + len(at.dataframe) > 0


def test_strategy_page_runs_the_simulator_and_lists_input_provenance(warehouse_path):
    at = _page("strategy", warehouse_path)
    assert not at.exception
    titles = [s.value for s in at.subheader]
    assert any("Monte Carlo" in t for t in titles)
    assert len(at.slider) == 2 and len(at.expander) == 1       # SC chance + pit loss; provenance expander


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
