"""Regression tests for the problems found when the dashboard screenshots were reviewed against the data (pure unit tests, no warehouse)."""
import numpy as np
import pandas as pd
import pytest
from f1_dash import charts, data
from f1_dash.charts._common import DRIVER_COLOURS, driver_colour, driver_colour_map


def test_a_driver_has_one_colour_on_every_chart_and_no_two_current_drivers_share_one():
    assert driver_colour("ANT") == driver_colour("ANT") == DRIVER_COLOURS["ANT"]
    assert len(set(DRIVER_COLOURS.values())) == len(DRIVER_COLOURS) == 22          # the 2026 grid, all different
    assert driver_colour("ZZZ") == driver_colour("ZZZ")                             # unknown drivers: stable, not random
    assert driver_colour_map(["VER", "ANT"]) == {"VER": DRIVER_COLOURS["VER"], "ANT": DRIVER_COLOURS["ANT"]}


def _traces(fig):
    return {t.name: t.line.color if t.line.color else t.marker.color for t in fig.data}


def test_line_charts_use_the_same_colour_for_the_same_driver_whatever_else_is_drawn():
    one = pd.DataFrame({"driver": ["VER"] * 3, "lap_number": [1, 2, 3], "gap_s": [1.0, 2.0, 3.0]})
    two = pd.concat([pd.DataFrame({"driver": ["ANT"] * 3, "lap_number": [1, 2, 3], "gap_s": [0.0] * 3}), one])
    assert _traces(charts.race_trace_fig(one))["VER"] == _traces(charts.race_trace_fig(two))["VER"] == DRIVER_COLOURS["VER"]


def test_pace_chart_breaks_the_rolling_line_where_laps_are_missing():
    laps = pd.DataFrame({"driver": "ANT", "lap_number": [8, 9, 14, 15], "lap_time_seconds": [97.0, 97.1, 98.0, 97.9], "is_representative_lap": True})
    fig = charts.pace_rolling_fig(laps, ["ANT"])
    line = next(t for t in fig.data if t.mode == "lines")
    ys = list(line.y)
    assert len(ys) == 8 and sum(np.isnan(v) for v in ys) == 4    # laps 10 to 13 are empty points: the line is not drawn across the safety car


def test_event_timeline_dims_future_events_and_draws_the_neutralisation_as_a_span():
    ev = pd.DataFrame({"lap": [10, 52], "end_lap": [13, np.nan], "kind": ["Neutralisation", "Fastest lap"], "driver": [None, "ANT"], "detail": ["SC", "1:35.275"]})
    fig = charts.event_timeline_fig(ev, current_lap=37)
    fastest = next(t for t in fig.data if t.name == "Fastest lap")
    assert list(fastest.marker.opacity) == [0.25]
    assert any(t.mode == "lines" and list(t.x) == [10, 13] for t in fig.data)


def test_a_replay_does_not_show_the_lap_a_penalty_will_be_served_on():
    pn = pd.DataFrame({"lap_number": [37, 60], "driver": ["OCO", "X"], "served_lap": [51.0, None]})
    now = data.penalties_so_far(pn, 37)
    assert len(now) == 1 and now["served_lap"].isna().all()
    later = data.penalties_so_far(pn, 51)
    assert later["served_lap"].iloc[0] == 51


def test_strategy_chart_has_no_bars_on_a_truncated_axis():
    fig = charts.strategy_fig([{"strategy": "A", "mean_s": 5761.9, "p10_s": 5621.0, "p90_s": 5986.0}, {"strategy": "B", "mean_s": 5762.0, "p10_s": 5622.0, "p90_s": 5990.0}])
    assert not any(t.type == "bar" for t in fig.data)


def test_sector_heatmap_keeps_the_fastest_drivers_even_when_none_of_their_laps_is_flagged_representative():
    rows = []
    for drv, base in (("ANT", 92.0), ("VER", 93.0)):
        for lap in (1, 2, 3):
            rows.append({"driver": drv, "lap_number": lap, "lap_time_seconds": base + lap * 0.1, "s1": 30 + lap * 0.01, "s2": 31.0, "s3": 31.0,
                         "is_in_lap": False, "is_out_lap": drv == "ANT", "is_representative_lap": drv == "VER"})   # real qualifying data: push laps flagged out-laps
    fig = charts.sector_heatmap_fig(pd.DataFrame(rows))
    assert set(fig.data[0].y) == {"ANT", "VER"}


def test_hazard_legend_names_the_event_and_the_sample_not_the_circuit_key():
    df = pd.DataFrame({"circuit_key": [49] * 2, "track_status": ["SC", "VSC"], "laps_remaining": [20, 20], "p_at_least_one_start": [0.2, 0.1],
                       "races_observed": [3, 3], "low_sample": [False, False]})
    names = {t.name for t in charts.sc_hazard_fig(df).data}
    assert names == {"SC (3 races)", "VSC (3 races)"}


def test_telemetry_overlay_breaks_the_line_across_a_dropout_and_labels_an_incomplete_lap():
    d = pd.DataFrame({"distance_m": [0, 10, 20, 30, 400, 410, 420], "speed_kph": [170.0] * 7, "throttle": 50.0, "brake": 0.0, "gear": 5, "is_complete_lap": False})
    fig = charts.telemetry_overlay_fig(d, pd.DataFrame(), "VER L19", "")
    speed = fig.data[0]
    assert "(telemetry gaps)" in speed.name and any(np.isnan(v) for v in speed.y)


# ---- the four items that were left open, plus the per-lap telemetry summary ----
import duckdb
from f1_dash import insights
from f1_dash.components import simulator


def test_a_pace_fit_that_significantly_improves_is_not_a_degradation_model_but_a_flat_one_is_kept():
    fits = pd.DataFrame({"session_name": "Race", "clean_laps": [14, 14, 14, 14, 6], "fresh_tyre_pace_seconds": 95.0,
                         "degradation_seconds_per_lap": [-0.05, -0.01, 0.0, 0.05, -0.05], "r_squared": [0.5, 0.01, 0.0, 0.5, 0.5]})
    assert list(insights.usable_pace_fits(fits)) == [False, True, True, True, False]     # last: too few laps


def test_anomaly_screen_uses_fuel_corrected_time_so_fuel_burn_is_not_flagged_as_unusual():
    laps = pd.DataFrame({"driver_number": 1, "lap_number": range(1, 31), "is_in_lap": False, "is_out_lap": False, "track_status": "GREEN"})
    laps["fuel_corrected_lap_time_seconds"] = 95.0 + np.tile([0.0, 0.05, -0.05], 10)
    laps["lap_time_seconds"] = laps["fuel_corrected_lap_time_seconds"] + 3.0 - 0.1 * laps["lap_number"]     # raw laps get 3 s faster over the race
    raw_only = insights.robust_anomalies(laps.drop(columns="fuel_corrected_lap_time_seconds"))
    corrected = insights.robust_anomalies(laps)
    trend = lambda d: abs(np.corrcoef(d["lap_number"], d["z"])[0, 1])
    assert trend(raw_only) > 0.9          # on raw times the z-score is just a ramp from the first lap to the last (fuel burning off), as on the real data
    assert trend(corrected) < 0.3 and not corrected["is_anomaly"].any()


def _warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE dim_driver_session (session_key INT, driver_number INT, driver_acronym VARCHAR)")
    con.execute("INSERT INTO dim_driver_session VALUES (1, 1, 'ANT'), (1, 2, 'VER')")
    con.execute("""CREATE TABLE fct_overtakes (session_key INT, lap_number INT, overtaking_driver_number INT, overtaken_driver_number INT, post_overtake_position INT,
                   was_in_drs_range_before_pass BOOLEAN, is_probable_pit_cycle_pass BOOLEAN, overtaker_tyre_compound VARCHAR, tyre_age_delta INT)""")
    con.execute("INSERT INTO fct_overtakes VALUES (1, 1, 1, 2, 15, TRUE, FALSE, 'SOFT', 0), (1, 1, 2, 1, 15, TRUE, FALSE, 'SOFT', 0), (1, 7, 1, 2, 3, FALSE, FALSE, 'SOFT', 2)")
    con.execute("CREATE TABLE fct_telemetry_lap_summary (session_key INT, driver_number INT, lap_number INT, top_speed_kph DOUBLE, min_speed_kph DOUBLE, avg_speed_kph DOUBLE,"
                " full_throttle_fraction DOUBLE, braking_fraction DOUBLE, braking_zone_count INT, drs_open_fraction DOUBLE, max_gear INT)")
    con.execute("INSERT INTO fct_telemetry_lap_summary VALUES (1, 1, 1, 330, 80, 200, .6, .2, 5, 0, 8), (1, 1, 2, 270, 60, 150, .1, .3, 5, 0, 7)")
    con.execute("CREATE TABLE fct_lap_performance (session_key INT, driver_number INT, lap_number INT, is_in_lap BOOLEAN, is_out_lap BOOLEAN, is_representative_lap BOOLEAN)")
    con.execute("INSERT INTO fct_lap_performance VALUES (1, 1, 1, FALSE, FALSE, TRUE), (1, 1, 2, TRUE, FALSE, FALSE)")
    return con


def test_overtakes_hide_the_lap_1_start_shuffle_unless_asked():
    con = _warehouse()
    assert list(data.overtakes(con, 1)["lap_number"]) == [1, 1, 7]
    assert list(data.overtakes(con, 1, include_first_lap=False)["lap_number"]) == [7]


def test_lap_summary_carries_the_push_lap_flags_so_a_page_can_hide_in_and_out_laps():
    ts = data.telemetry_lap_summary(_warehouse(), 1)
    assert list(ts["is_representative_lap"]) == [True, False] and list(ts["is_in_lap"]) == [False, True]


def test_the_simulator_ranking_is_meaningless_only_when_every_compound_is_clipped():
    assert simulator.ranking_is_meaningless(["HARD", "MEDIUM"], {"HARD": 1, "MEDIUM": 2})
    assert not simulator.ranking_is_meaningless(["MEDIUM"], {"HARD": 1, "MEDIUM": 2})
    assert not simulator.ranking_is_meaningless([], {"HARD": 1, "MEDIUM": 2})
