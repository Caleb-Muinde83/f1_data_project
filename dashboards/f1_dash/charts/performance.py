"""Timing and pace charts (analyst)."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ._common import COMPOUND_COLOURS, empty, lacks


def race_trace_fig(df: pd.DataFrame):
    if df.empty:
        return empty("No race trace for this session")
    fig = px.line(df, x="lap_number", y="gap_s", color="driver", markers=True, labels={"lap_number": "Lap", "gap_s": "Gap to leader (s)"})
    fig.update_yaxes(autorange="reversed")  # leader on top, like a race trace
    return fig


def lap_scatter_fig(df: pd.DataFrame, representative_only: bool = True, fuel_corrected: bool = True):
    if lacks(df, "is_representative_lap", "fuel_corrected_lap_time_seconds", "lap_time_seconds"):
        return empty("No lap data for this session")
    d = df[df["is_representative_lap"]] if representative_only else df
    y = "fuel_corrected_lap_time_seconds" if fuel_corrected and d["fuel_corrected_lap_time_seconds"].notna().any() else "lap_time_seconds"
    if d.empty:
        return empty("No representative laps")
    return px.scatter(d, x="lap_number", y=y, color="tyre_compound", symbol="driver", color_discrete_map=COMPOUND_COLOURS,
                      labels={"lap_number": "Lap", y: "Lap time (s)"})


def degradation_fig(df: pd.DataFrame):
    if lacks(df, "degradation_seconds_per_lap"):
        return empty("Not enough clean laps to estimate degradation")
    d = df[df["degradation_seconds_per_lap"].notna()]
    if d.empty:
        return empty("Not enough clean laps to estimate degradation")
    return px.bar(d, x="driver", y="degradation_seconds_per_lap", color="tyre_compound", barmode="group",
                  color_discrete_map=COMPOUND_COLOURS, labels={"degradation_seconds_per_lap": "Degradation (s per lap of tyre age)"})


def points_fig(df: pd.DataFrame):
    if df.empty:
        return empty("No points data for this season")
    return px.line(df, x="session_start_time", y="cumulative_points", color="driver", markers=True, labels={"cumulative_points": "Points", "session_start_time": ""})


def position_progression_fig(df: pd.DataFrame, highlight=None):
    """Every driver's position on every lap."""
    if df.empty:
        return empty("No position data for this session")
    fig = px.line(df, x="lap_number", y="position", color="driver", markers=False, labels={"lap_number": "Lap", "position": "Position"})
    fig.update_yaxes(autorange="reversed", dtick=1)
    if highlight:
        fig.update_traces(opacity=0.55, selector=lambda t: t.name not in highlight)
        fig.update_traces(line_width=4, selector=lambda t: t.name in highlight)
    return fig


def gap_evolution_fig(df: pd.DataFrame, drivers):
    """Gap to the car ahead lap by lap for the chosen drivers (the dirty-air / DRS window is shaded)."""
    if lacks(df, "driver", "gap_to_car_ahead_end_seconds"):
        return empty("No gap data for these drivers")
    d = df[df["driver"].isin(drivers) & df["gap_to_car_ahead_end_seconds"].notna()]
    if d.empty:
        return empty("No gap data for these drivers")
    fig = px.line(d, x="lap_number", y="gap_to_car_ahead_end_seconds", color="driver", markers=True,
                  labels={"lap_number": "Lap", "gap_to_car_ahead_end_seconds": "Gap to car ahead (s)"})
    fig.add_hrect(y0=0, y1=1.0, fillcolor="#39b54a", opacity=0.12, line_width=0, annotation_text="within 1.0s (DRS-range proxy)")
    return fig


def pace_rolling_fig(df: pd.DataFrame, drivers, window: int = 3):
    """Lap time with a rolling average, clean laps only."""
    if lacks(df, "driver", "is_representative_lap", "lap_time_seconds"):
        return empty("No representative laps for these drivers")
    d = df[df["driver"].isin(drivers) & df["is_representative_lap"] & df["lap_time_seconds"].notna()].sort_values(["driver", "lap_number"])
    if d.empty:
        return empty("No representative laps for these drivers")
    d = d.assign(rolling=d.groupby("driver")["lap_time_seconds"].transform(lambda s: s.rolling(window, min_periods=1).mean()))
    fig = go.Figure()
    for i, (drv, g) in enumerate(d.groupby("driver")):
        colour = px.colors.qualitative.Set2[i % 8]
        fig.add_scatter(x=g["lap_number"], y=g["lap_time_seconds"], mode="markers", name=f"{drv} lap", marker_color=colour, opacity=0.5)
        fig.add_scatter(x=g["lap_number"], y=g["rolling"], mode="lines", name=f"{drv} rolling {window}", line={"color": colour, "width": 3})
    fig.update_layout(xaxis_title="Lap", yaxis_title="Lap time (s)")
    fig.update_xaxes(dtick=1)
    return fig


def sector_heatmap_fig(df: pd.DataFrame, max_ratio: float = 1.07, clip_seconds: float = 1.5):
    """Best sector per driver relative to the session-best sector. Drivers whose best lap is slower than `max_ratio` x the session best (no real push
    lap) are hidden, and the colour scale is clipped at `clip_seconds`, so a few 14 s outliers do not turn every other cell the same green."""
    cols = ["s1", "s2", "s3"]
    if lacks(df, "driver", "is_representative_lap", "lap_time_seconds", *cols):
        return empty("No sector times for this session")
    rep = df[df["is_representative_lap"]]
    best_lap = rep.groupby("driver")["lap_time_seconds"].min()
    if best_lap.empty:
        return empty("No sector times for this session")
    keep = best_lap[best_lap <= max_ratio * best_lap.min()].index
    d = rep[rep["driver"].isin(keep)].groupby("driver")[cols].min()
    if d.empty or d.isna().all().all():
        return empty("No sector times for this session")
    loss = d - d.min()
    fig = go.Figure(go.Heatmap(z=loss.values, x=["Sector 1", "Sector 2", "Sector 3"], y=list(loss.index), colorscale="RdYlGn_r", zmin=0, zmax=clip_seconds,
                               colorbar={"title": f"s lost (clipped at {clip_seconds})"}, text=loss.round(2).astype(str).values, texttemplate="%{text}",
                               hovertemplate="%{y} %{x}: +%{z:.3f}s<extra></extra>"))
    fig.update_yaxes(autorange="reversed")
    return fig


def event_timeline_fig(events: pd.DataFrame, current_lap: int = None):
    """Every race event on one lap axis (one row per kind). `current_lap` draws the replay clock."""
    if lacks(events, "lap", "kind", "detail"):
        return empty("No events recorded for this session")
    fig = px.scatter(events.assign(driver=events["driver"].fillna("")), x="lap", y="kind", color="kind", hover_data={"detail": True, "driver": True, "kind": False},
                     labels={"lap": "Lap", "kind": ""})
    fig.update_traces(marker={"size": 11, "symbol": "diamond"})
    if current_lap is not None:
        fig.add_vline(x=current_lap, line_dash="dot", line_color="#e10600", annotation_text="replay")
    fig.update_layout(showlegend=False, height=260, margin={"t": 20})
    return fig
