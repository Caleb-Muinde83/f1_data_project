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
