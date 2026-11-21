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
