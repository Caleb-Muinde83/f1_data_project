"""Pure unit tests (no warehouse): the analytics helpers behind the Data Scientist workspace and the alerts."""
import numpy as np
import pandas as pd
import pytest
from f1_dash import insights, charts


def _hist():
    # three earlier starts from P1: win, 2nd, and a retirement
    return pd.DataFrame({"session_key": [1, 1, 1], "grid_position": [1, 1, 1], "final_position": [1, 2, None],
                         "classification_status": ["Finished", "Finished", "DNF"]})


def test_grid_baseline_probabilities_sum_to_one_and_put_dnfs_in_their_own_bucket():
    d = insights.grid_baseline(_hist(), 1, window=0)
    assert d.probability.sum() == pytest.approx(1.0) and d.n.iloc[0] == 3
    assert d.loc[d.finish == insights.DNF_BUCKET, "probability"].iloc[0] == pytest.approx(1 / 3)
    assert insights.expected_finish(d) == pytest.approx(1.5)        # DNFs are excluded from the mean position
    assert insights.p_at_most(d, 3) == pytest.approx(2 / 3)


def test_grid_baseline_with_no_matching_history_is_empty_not_an_error():
    assert insights.grid_baseline(_hist(), 15, window=0).empty
    assert insights.grid_baseline(pd.DataFrame(), 3).empty


def _stint(n=8, slope=0.05, drv=44):
    ages = np.arange(1, n + 1)
    return pd.DataFrame({"driver_number": drv, "lap_number": ages + 10, "stint_number": 2, "tyre_compound": "MEDIUM", "tyre_age_this_lap": ages,
                         "is_representative_lap": True, "fuel_corrected_lap_time_seconds": 90 + slope * ages, "lap_time_seconds": 90 + slope * ages})


def test_degradation_fit_recovers_the_slope_and_only_uses_laps_seen_so_far():
    laps = _stint()
    fit = insights.fit_stint_degradation(laps, 44, upto_lap=15)       # replay clock at lap 15: laps 11..15 only
    assert fit["slope"] == pytest.approx(0.05, abs=1e-6) and fit["n"] == 5
    assert max(fit["lap"]) <= 15                                      # no look-ahead
    assert insights.laps_until_loss(fit, 1.0) == 20


def test_degradation_fit_needs_enough_clean_laps():
    assert insights.fit_stint_degradation(_stint(n=3), 44, upto_lap=99) is None
    assert insights.fit_stint_degradation(_stint(), 7, upto_lap=99) is None


def test_projection_chart_handles_none():
    assert len(charts.degradation_projection_fig(None).layout.annotations) == 1
    assert len(charts.degradation_projection_fig(insights.fit_stint_degradation(_stint(), 44, 99)).data) >= 2


def test_robust_anomalies_flags_a_spin_lap_but_not_normal_scatter():
    t = np.r_[90 + 0.2 * np.sin(np.arange(29)), 110.0]          # deterministic +-0.2 s wobble, then one 20 s lap
    laps = pd.DataFrame({"driver_number": 1, "lap_number": range(1, 31), "lap_time_seconds": t, "is_in_lap": False, "is_out_lap": False})
    d = insights.robust_anomalies(laps)
    assert list(d.loc[d.is_anomaly, "lap_number"]) == [30]


def test_error_metrics_ignore_missing_values():
    m = insights.error_metrics([1, 2, 3, np.nan], [1, 3, 5, 9])
    assert m["n"] == 3 and m["mae"] == pytest.approx(1.0) and m["bias"] == pytest.approx(-1.0) and m["rmse"] == pytest.approx(np.sqrt(5 / 3))
    assert insights.error_metrics([], [])["n"] == 0


def test_pace_model_residuals_apply_the_dbt_line():
    laps = _stint().assign(driver="HAM")
    model = pd.DataFrame({"driver": ["HAM"], "tyre_compound": ["MEDIUM"], "fresh_tyre_pace_seconds": [90.0], "degradation_seconds_per_lap": [0.05]})
    r = insights.pace_model_residuals(laps, model)
    assert len(r) == 8 and r.residual.abs().max() < 1e-9


def test_engineering_alerts_skip_lap_one_and_non_green_laps():
    laps = pd.DataFrame({"driver_number": 1, "lap_number": [1, 2, 3], "delta_to_driver_median_seconds": [6.0, 2.0, 2.0], "is_in_lap": False,
                         "is_out_lap": False, "track_status": ["GREEN", "GREEN", "SC"]})
    pen = pd.DataFrame({"lap_number": [2], "penalty_type": ["TIME"], "reason": ["track limits"]})
    out = insights.engineering_alerts(laps, pen, pd.DataFrame(), 1, upto_lap=3)
    assert [(s, l) for s, l, _ in out] == [("warn", 2), ("crit", 2)]
    assert insights.engineering_alerts(laps, pen, pd.DataFrame(), 1, upto_lap=1) == []


def test_every_figure_function_returns_a_message_for_empty_input():
    e = pd.DataFrame()
    for f in (charts.finishing_distribution_fig, ):
        assert len(f(e, 1).layout.annotations) == 1
    for f in (charts.position_progression_fig, charts.circuit_map_fig, charts.anomaly_fig, charts.residual_fig, charts.pit_duration_fig, charts.sector_heatmap_fig,
              charts.degradation_fig, charts.corner_fig, charts.lap_scatter_fig):
        assert len(f(e).layout.annotations) == 1
    assert len(charts.telemetry_overlay_fig(e, e, "a", "b").layout.annotations) == 1
    assert len(charts.gap_evolution_fig(e, ["A"]).layout.annotations) == 1 and len(charts.pace_rolling_fig(e, ["A"]).layout.annotations) == 1


def test_anomaly_screen_ignores_safety_car_laps():
    t = np.r_[90 + 0.2 * np.sin(np.arange(20)), 140.0, 141.0]          # two SC-speed laps at the end
    laps = pd.DataFrame({"driver_number": 1, "lap_number": range(1, 23), "lap_time_seconds": t, "is_in_lap": False, "is_out_lap": False,
                         "track_status": ["GREEN"] * 20 + ["SC", "SC"]})
    assert not insights.robust_anomalies(laps).is_anomaly.any() and len(insights.robust_anomalies(laps)) == 20


def test_timing_tower_from_cumulative_order_never_repeats_a_position():
    from f1_dash.components import timing
    laps = pd.DataFrame({"driver_number": [1, 2], "driver": ["A", "B"], "team": "T", "lap_number": 5, "lap_time_seconds": 90.0, "tyre_compound": "HARD",
                         "tyre_age_this_lap": 3, "track_status": "GREEN", "position_end": [2, 2],         # the feed says P2 twice
                         "gap_to_leader_end_seconds": [None, 1.0], "gap_to_car_ahead_end_seconds": [None, 1.0]})
    order = pd.DataFrame({"lap_number": 5, "driver_number": [1, 2], "position": [1, 2], "gap_to_leader": [0.0, 1.5], "interval": [None, 1.5]})
    t = timing.tower(laps, 5, order=order)
    assert list(t["Pos"]) == [1, 2] and list(t["Driver"]) == ["A", "B"]


def test_stint_fit_is_not_tilted_by_slow_restart_laps():
    """Real data (Shanghai 2026): three slow laps after a safety-car restart turned a flat stint into a -0.07 s/lap 'improvement' under least squares."""
    ages = np.arange(1, 14)
    y = np.r_[99.1, 97.5, 96.1, np.full(10, 95.4) + 0.005 * np.arange(10)]
    laps = pd.DataFrame({"driver_number": 63, "lap_number": ages + 13, "stint_number": 2, "tyre_compound": "HARD", "tyre_age_this_lap": ages,
                         "is_representative_lap": True, "fuel_corrected_lap_time_seconds": y, "lap_time_seconds": y})
    ols = np.polyfit(ages, y, 1)[0]
    fit = insights.fit_stint_degradation(laps, 63, upto_lap=99)
    assert ols < -0.15 and abs(fit["slope"]) < 0.03       # least squares is badly tilted, the robust fit is not
    assert fit["sigma"] < 0.2 and fit["n"] <= 12


def _pace_rows():
    # numbers taken from the real Shanghai practice/qualifying/sprint tables
    rows = [("Practice 1", "RUS", "MEDIUM", 9, 93.6014, 0.6186), ("Practice 1", "ALB", "MEDIUM", 5, 96.3535, 0.8266), ("Practice 1", "BOR", "MEDIUM", 6, 114.5447, -2.2635),
            ("Sprint Qualifying", "COL", "MEDIUM", 6, 164.7368, -16.7485), ("Sprint", "ALO", "HARD", 14, 99.3811, 0.0088), ("Sprint", "BOR", "MEDIUM", 14, 98.5845, 0.0233),
            ("Qualifying", "LIN", "SOFT", 5, 109.9368, 0.4788)]
    return pd.DataFrame(rows, columns=["session_name", "driver", "tyre_compound", "clean_laps", "fresh_tyre_pace_seconds", "degradation_seconds_per_lap"]).assign(
        session_key=lambda d: d.session_name.map({"Practice 1": 1, "Sprint Qualifying": 2, "Sprint": 3, "Qualifying": 4}), r_squared=0.01)


def test_usable_pace_fits_drops_the_junk_but_keeps_flat_valid_stints():
    df = _pace_rows()
    ok = df[insights.usable_pace_fits(df)]
    assert set(ok.driver + ok.session_name) == {"RUSPractice 1", "ALOSprint", "BORSprint"}      # junk (164 s, 114 s, <8 laps) is gone; r2 ~ 0 does not matter
    assert insights.best_calibration_session(df) == 3                                               # the Sprint has two compounds with usable fits


def test_sector_heatmap_hides_drivers_without_a_push_lap_and_clips_the_scale():
    rows = []
    for drv, lap, s in (("A", 92.0, (28.0, 33.0, 31.0)), ("B", 92.5, (28.2, 33.1, 31.2)), ("C", 99.0, (30.0, 36.0, 45.0))):   # C never pushed (107%+)
        rows.append({"driver": drv, "is_representative_lap": True, "lap_time_seconds": lap, "s1": s[0], "s2": s[1], "s3": s[2]})
    fig = charts.sector_heatmap_fig(pd.DataFrame(rows))
    hm = fig.data[0]
    assert list(hm.y) == ["A", "B"] and hm.zmax == 1.5


def test_field_median_corners_gives_chosen_drivers_plus_one_median_bar_per_corner():
    cn = pd.DataFrame({"corner_number": [1, 1, 1, 2, 2, 2], "driver": ["A", "B", "C"] * 2, "avg_apex_kph": [90.0, 100.0, 110.0, 120.0, 130.0, 140.0]})
    out = insights.field_median_corners(cn, ["A"])
    assert sorted(out.driver.unique()) == ["A", "FIELD MEDIAN"] and out[out.driver == "FIELD MEDIAN"].avg_apex_kph.tolist() == [100.0, 130.0]


def test_pit_markers_are_visible_on_light_compounds():
    st_ = pd.DataFrame({"driver": ["A"], "stint_number": [1], "tyre_compound": ["HARD"], "first_lap": [1], "last_lap": [20]})
    fig = charts.stint_gantt_fig(st_, pd.DataFrame({"driver": ["A"], "lap_number": [10]}))
    assert fig.data[-1].marker.color != "#fff"


def test_fit_diagnostics_explain_why_there_is_nothing_to_fit():
    laps = pd.DataFrame({"driver_number": 63, "lap_number": [1, 2, 3], "stint_number": 1, "tyre_compound": "HARD", "tyre_age_this_lap": [1, 2, 3],
                         "is_representative_lap": [False, True, True]})
    assert "no laps" in insights.fit_diagnostics(laps, 99, 10)
    msg = insights.fit_diagnostics(laps, 63, 3)
    assert "3 laps so far" in msg and "2 are clean" in msg and "at least 4" in msg


def test_simulator_names_compounds_whose_degradation_was_clipped():
    from f1_dash.components import simulator      # the strings below are the ones the real Shanghai run produced
    prov = {"MEDIUM.degradation": "clipped from -0.0222 to 0 (negative slope = noise or track evolution)", "HARD.degradation": "clipped from -0.0115 to 0 (negative slope)",
            "SOFT.degradation": "measured", "pit_loss": "measured: circuit median"}
    assert simulator.clipped_compounds(prov) == ["HARD", "MEDIUM"] and simulator.clipped_compounds({}) == []


def test_event_timeline_handles_events_and_none():
    ev = pd.DataFrame({"lap": [10, 12], "kind": ["Neutralisation", "Pit stop"], "driver": [None, "ALO"], "detail": ["SC for 4 laps", "MEDIUM > HARD"]})
    assert len(charts.event_timeline_fig(ev, 11).data) >= 1 and len(charts.event_timeline_fig(pd.DataFrame()).layout.annotations) == 1


def test_big_line_charts_are_rendered_as_svg_not_webgl():
    """Plotly Express switches to WebGL above ~1000 rows; WebGL lines failed to draw (position progression empty, race trace as loose dots)."""
    import plotly.express as px
    from f1_dash.components import plot
    n = 20 * 56
    df = pd.DataFrame({"lap_number": np.tile(np.arange(1, 57), 20), "position": np.tile(np.arange(1, 21), 56), "driver": np.repeat([f"D{i}" for i in range(20)], 56)})
    fig = px.line(df, x="lap_number", y="position", color="driver")
    assert len(df) > 1000 and fig.data[0].type == "scattergl"          # what the charts produced before
    svg = plot.to_svg(fig)
    assert {t.type for t in svg.data} == {"scatter"} and len(svg.data) == 20
    a, b = svg.to_dict()["data"][0], fig.to_dict()["data"][0]
    assert a["x"] == b["x"] and a["y"] == b["y"] and a["mode"] == b["mode"] == "lines" and a["name"] == b["name"]     # same data, same style, only the trace type changes
    small = px.line(df.head(50), x="lap_number", y="position", color="driver")
    assert plot.to_svg(small) is small                                  # untouched when nothing needs converting


def test_a_five_minute_retirement_lap_does_not_flatten_the_anomaly_chart():
    """Real 2026 Shanghai: VER's retirement lap scored z = 60 and squashed every other lap against the axis."""
    df = pd.DataFrame({"lap_number": [1, 2, 3, 4], "z": [0.2, -0.4, 1.0, 60.0], "is_anomaly": [False, False, False, True]})
    fig = charts.anomaly_fig(df)
    ys = [float(v) for t in fig.data for v in t.y]
    assert max(ys) <= 12 and 60.0 in [float(c[0]) for t in fig.data for c in t.customdata]     # drawn clipped, hover keeps the true score
