"""Predictive and model-evaluation charts (data scientist)."""
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ._common import driver_colour_map, empty, lacks


def sc_hazard_fig(df: pd.DataFrame):
    if df.empty:
        return empty("No safety car model rows")
    # The legend used to show the circuit key ("49 SC"), which means nothing to a reader: show the event type and how many races the rate rests on.
    multi = df["circuit_key"].nunique() > 1
    d = df.assign(series=df["track_status"] + " (" + df["races_observed"].astype(str) + " races" + df["low_sample"].map({True: ", low sample", False: ""}).fillna("")
                  + ")" + (" circuit " + df["circuit_key"].astype(str) if multi else ""))
    return px.line(d, x="laps_remaining", y="p_at_least_one_start", color="series", markers=True,
                   labels={"laps_remaining": "Laps remaining", "p_at_least_one_start": "P(at least one start)"})


def finishing_distribution_fig(dist: pd.DataFrame, grid: int):
    if dist.empty:
        return empty("No earlier races to build a base rate from")
    fig = px.bar(dist, x="finish", y="probability", labels={"finish": "Finishing position (21 = DNF/other)", "probability": "Probability"})
    fig.update_traces(marker_color="#b983ff"); fig.update_yaxes(tickformat=".0%")
    fig.update_layout(title=f"Base rate: finishing position from P{grid} on the grid")
    return fig


def degradation_projection_fig(fit: dict):
    if not fit:
        return empty("Not enough clean laps in the current stint to fit a degradation line")
    fig = go.Figure()
    fig.add_scatter(x=fit["lap"], y=fit["actual"], mode="markers", name="observed (clean laps)", marker_color="#3da5ff")
    fig.add_scatter(x=fit["proj_lap"], y=fit["proj"], mode="lines", name="projection", line={"color": "#b983ff", "width": 3})
    fig.add_scatter(x=list(fit["proj_lap"]) + list(fit["proj_lap"])[::-1], y=list(fit["hi"]) + list(fit["lo"])[::-1], fill="toself",
                    fillcolor="rgba(185,131,255,0.15)", line={"width": 0}, name="~90% interval", hoverinfo="skip")
    fig.update_layout(xaxis_title="Lap", yaxis_title="Fuel-corrected lap time (s)")
    return fig


def anomaly_fig(df: pd.DataFrame):
    if lacks(df, "lap_number", "z", "is_anomaly"):
        return empty("No laps to score")
    # A retirement lap can score z = 60 and flatten every other point: the axis is clipped at +-12 (hover still shows the true score).
    d = df.assign(z_shown=df["z"].clip(-12, 12))
    fig = px.scatter(d, x="lap_number", y="z_shown", color=d["is_anomaly"].map({True: "anomalous", False: "normal"}), hover_data={"z": ":.1f", "z_shown": False},
                     color_discrete_map={"anomalous": "#e10600", "normal": "#7a8699"}, labels={"z_shown": "Robust z-score of lap time (axis clipped at 12)", "lap_number": "Lap", "color": "Lap"})
    fig.add_hline(y=3, line_dash="dot"); fig.add_hline(y=-3, line_dash="dot")
    return fig


def residual_fig(res: pd.DataFrame):
    if lacks(res, "lap_number", "residual", "driver"):
        return empty("No fitted laps to evaluate")
    fig = px.scatter(res, x="lap_number", y="residual", color="driver", color_discrete_map=driver_colour_map(res["driver"].unique()), labels={"residual": "Actual - predicted lap time (s)", "lap_number": "Lap"})
    fig.add_hline(y=0, line_dash="dot")
    return fig


def predicted_vs_actual_fig(df: pd.DataFrame, x: str, y: str, label_x: str, label_y: str):
    if df.empty:
        return empty("Nothing to compare")
    fig = px.scatter(df, x=x, y=y, text="driver" if "driver" in df else None, labels={x: label_x, y: label_y})
    lo, hi = float(min(df[x].min(), df[y].min())), float(max(df[x].max(), df[y].max()))
    fig.add_scatter(x=[lo, hi], y=[lo, hi], mode="lines", line={"dash": "dot", "color": "#8b93a1"}, name="perfect prediction")
    fig.update_traces(textposition="top center", selector={"mode": "markers+text"})
    return fig
