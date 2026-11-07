"""Tyre and pit strategy charts."""
import pandas as pd
import plotly.graph_objects as go
from ._common import COMPOUND_COLOURS, empty, lacks


def stint_gantt_fig(df: pd.DataFrame, pits: pd.DataFrame = None):
    if df.empty:
        return empty("No stint data")
    d = df.assign(length=df["last_lap"] - df["first_lap"] + 1)
    fig = go.Figure()
    for comp, g in d.groupby("tyre_compound"):
        fig.add_bar(y=g["driver"], x=g["length"], base=g["first_lap"] - 1, orientation="h", name=str(comp),
                    marker_color=COMPOUND_COLOURS.get(comp, "#888888"),
                    hovertext=[f"stint {s}: laps {a}-{b}" for s, a, b in zip(g["stint_number"], g["first_lap"], g["last_lap"])])
    if pits is not None and not pits.empty:
        fig.add_scatter(x=pits["lap_number"], y=pits["driver"], mode="markers", name="pit stop", marker={"symbol": "line-ns", "size": 16, "color": "#fff", "line": {"width": 2}})
    fig.update_layout(barmode="overlay", xaxis_title="Lap", yaxis={"autorange": "reversed"})
    return fig


def strategy_fig(rows: list):
    if not rows:
        return empty("No simulation result")
    df = pd.DataFrame(rows)
    fig = go.Figure()
    fig.add_bar(y=df["strategy"], x=df["mean_s"], orientation="h", name="mean",
                error_x={"type": "data", "symmetric": False, "array": df["p90_s"] - df["mean_s"], "arrayminus": df["mean_s"] - df["p10_s"]})
    fig.update_layout(yaxis={"autorange": "reversed"}, xaxis_title="Total race time (s)  (bars: P10 to P90)")
    lo = float(df["p10_s"].min())
    fig.update_xaxes(range=[lo - 5, float(df["p90_s"].max()) + 5])
    return fig


def pit_duration_fig(df: pd.DataFrame):
    if lacks(df, "driver", "stop_number", "pit_lane_time_seconds", "is_slow_stop"):
        return empty("No pit stops")
    fig = go.Figure()
    fig.add_bar(x=df["driver"] + " #" + df["stop_number"].astype(str), y=df["pit_lane_time_seconds"], name="pit-lane time",
                marker_color=["#e10600" if s else "#3da5ff" for s in df["is_slow_stop"].fillna(False)])
    fig.update_layout(yaxis_title="Pit-lane time (s): not stationary time", xaxis_title="")
    return fig
