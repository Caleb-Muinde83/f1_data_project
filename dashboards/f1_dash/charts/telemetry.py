"""Telemetry and driver-input charts (engineer)."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from ._common import driver_colour, driver_colour_map, empty, lacks


def delta_trace_fig(df: pd.DataFrame):
    if df.empty:
        return empty("No telemetry delta trace (needs telemetry for the fastest laps)")
    fig = px.line(df, x="distance_m", y="time_delta_seconds", color="driver", color_discrete_map=driver_colour_map(df["driver"].unique()), labels={"distance_m": "Distance along lap (m)", "time_delta_seconds": "Delta to fastest lap (s)"})
    fig.add_hline(y=0, line_dash="dot")
    return fig


def corner_fig(df: pd.DataFrame):
    if lacks(df, "driver", "avg_apex_kph"):
        return empty("No corner data")
    d = df[df["driver"].notna()]
    if d.empty:
        return empty("No corner data")
    return px.bar(d, x="corner_number", y="avg_apex_kph", color="driver", color_discrete_map={**driver_colour_map(d["driver"].unique()), "FIELD MEDIAN": "#ffd12e"}, barmode="group", labels={"corner_number": "Detected corner #", "avg_apex_kph": "Apex speed (km/h)"})


def _break_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Insert an empty row wherever telemetry has a hole (a dropout), so the line is broken there instead of a straight segment being drawn across it.
    A flat stretch at the start or end of a lap on the real data was exactly such a segment."""
    d = df.sort_values("distance_m").reset_index(drop=True)
    if len(d) < 3:
        return d
    step = d["distance_m"].diff()
    thr = max(60.0, 4.0 * float(step.median()))
    gaps = d.index[step > thr]
    if len(gaps) == 0:
        return d
    fill = pd.DataFrame({"distance_m": (d.loc[gaps, "distance_m"] - step[gaps] / 2).to_numpy()})
    return pd.concat([d, fill], ignore_index=True).sort_values("distance_m", kind="stable").reset_index(drop=True)


def _lap_colour(label: str, fallback: str) -> str:
    first = str(label).split()[0] if str(label).strip() else ""
    return driver_colour(first) if first.isalpha() and first.isupper() and len(first) == 3 else fallback


def telemetry_overlay_fig(a: pd.DataFrame, b: pd.DataFrame, label_a: str, label_b: str):
    """Two laps over distance: speed, throttle, brake, gear. The engineer's core comparison."""
    if a.empty and b.empty:
        return empty("No telemetry for the selected laps")
    rows = [("speed_kph", "Speed (km/h)"), ("throttle", "Throttle (%)"), ("brake", "Brake"), ("gear", "Gear")]
    fig = make_subplots(rows=len(rows), cols=1, shared_xaxes=True, vertical_spacing=0.06, subplot_titles=[r[1] for r in rows])
    col_a, col_b = _lap_colour(label_a, "#e10600"), _lap_colour(label_b, "#3da5ff")
    if col_a == col_b:                       # two laps of the same driver must still be told apart
        col_a, col_b = "#e10600", "#3da5ff"
    for i, (col, _) in enumerate(rows, 1):
        for df, lbl, colour in ((a, label_a, col_a), (b, label_b, col_b)):
            if not df.empty:
                if "is_complete_lap" in df and not df["is_complete_lap"].fillna(True).all():
                    lbl = f"{lbl} (telemetry gaps)"
                df = _break_gaps(df)
                fig.add_scatter(x=df["distance_m"], y=df[col], name=lbl, legendgroup=lbl, showlegend=(i == 1), line={"color": colour, "width": 2}, connectgaps=False,
                                line_shape="hv" if col in ("gear", "brake") else "linear", row=i, col=1)
    fig.update_layout(height=620, hovermode="x unified"); fig.update_xaxes(title_text="Distance along lap (m)", row=len(rows), col=1)
    return fig


def lap_summary_fig(df: pd.DataFrame, metric: str, label: str):
    if lacks(df, metric):
        return empty("No telemetry summary")
    d = df[df[metric].notna()]
    if d.empty:
        return empty("No telemetry summary")
    return px.line(d, x="lap_number", y=metric, color="driver", color_discrete_map=driver_colour_map(d["driver"].unique()), markers=True, labels={"lap_number": "Lap", metric: label})


def input_scatter_fig(df: pd.DataFrame):
    """Throttle vs brake application along the lap for one driver (braking points are where brake rises)."""
    if df.empty:
        return empty("No telemetry for this lap")
    fig = go.Figure()
    fig.add_scatter(x=df["distance_m"], y=df["throttle"], name="Throttle %", line={"color": "#39b54a"})
    fig.add_scatter(x=df["distance_m"], y=df["brake"] * (100 if df["brake"].max() <= 1 else 1), name="Brake", line={"color": "#e10600"})
    fig.update_layout(xaxis_title="Distance (m)", yaxis_title="Input")
    return fig
