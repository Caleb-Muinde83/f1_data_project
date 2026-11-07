"""Telemetry and driver-input charts (engineer)."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from ._common import empty, lacks


def delta_trace_fig(df: pd.DataFrame):
    if df.empty:
        return empty("No telemetry delta trace (needs telemetry for the fastest laps)")
    fig = px.line(df, x="distance_m", y="time_delta_seconds", color="driver", labels={"distance_m": "Distance along lap (m)", "time_delta_seconds": "Delta to fastest lap (s)"})
    fig.add_hline(y=0, line_dash="dot")
    return fig


def corner_fig(df: pd.DataFrame):
    if lacks(df, "driver", "avg_apex_kph"):
        return empty("No corner data")
    d = df[df["driver"].notna()]
    if d.empty:
        return empty("No corner data")
    return px.bar(d, x="corner_number", y="avg_apex_kph", color="driver", barmode="group", labels={"corner_number": "Detected corner #", "avg_apex_kph": "Apex speed (km/h)"})


def telemetry_overlay_fig(a: pd.DataFrame, b: pd.DataFrame, label_a: str, label_b: str):
    """Two laps over distance: speed, throttle, brake, gear. The engineer's core comparison."""
    if a.empty and b.empty:
        return empty("No telemetry for the selected laps")
    rows = [("speed_kph", "Speed (km/h)"), ("throttle", "Throttle (%)"), ("brake", "Brake"), ("gear", "Gear")]
    fig = make_subplots(rows=len(rows), cols=1, shared_xaxes=True, vertical_spacing=0.03, subplot_titles=[r[1] for r in rows])
    for i, (col, _) in enumerate(rows, 1):
        for df, lbl, colour in ((a, label_a, "#e10600"), (b, label_b, "#3da5ff")):
            if not df.empty:
                fig.add_scatter(x=df["distance_m"], y=df[col], name=lbl, legendgroup=lbl, showlegend=(i == 1), line={"color": colour, "width": 2},
                                line_shape="hv" if col in ("gear", "brake") else "linear", row=i, col=1)
    fig.update_layout(height=620, hovermode="x unified"); fig.update_xaxes(title_text="Distance along lap (m)", row=len(rows), col=1)
    return fig


def lap_summary_fig(df: pd.DataFrame, metric: str, label: str):
    if lacks(df, metric):
        return empty("No telemetry summary")
    d = df[df[metric].notna()]
    if d.empty:
        return empty("No telemetry summary")
    return px.line(d, x="lap_number", y=metric, color="driver", markers=True, labels={"lap_number": "Lap", metric: label})


def input_scatter_fig(df: pd.DataFrame):
    """Throttle vs brake application along the lap for one driver (braking points are where brake rises)."""
    if df.empty:
        return empty("No telemetry for this lap")
    fig = go.Figure()
    fig.add_scatter(x=df["distance_m"], y=df["throttle"], name="Throttle %", line={"color": "#39b54a"})
    fig.add_scatter(x=df["distance_m"], y=df["brake"] * (100 if df["brake"].max() <= 1 else 1), name="Brake", line={"color": "#e10600"})
    fig.update_layout(xaxis_title="Distance (m)", yaxis_title="Input")
    return fig
