"""Pure DataFrame -> plotly Figure functions (unit tested, no Streamlit or database dependency)."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

COMPOUND_COLOURS = {"SOFT": "#e10600", "MEDIUM": "#ffd12e", "HARD": "#dddddd", "INTERMEDIATE": "#39b54a", "WET": "#1e6fd9", "UNKNOWN": "#888888"}


def _empty(msg: str):
    fig = go.Figure()
    fig.add_annotation(text=msg, showarrow=False, font={"size": 16})
    fig.update_xaxes(visible=False); fig.update_yaxes(visible=False)
    return fig


def race_trace_fig(df: pd.DataFrame):
    if df.empty:
        return _empty("No race trace for this session")
    fig = px.line(df, x="lap_number", y="gap_s", color="driver", markers=True, labels={"lap_number": "Lap", "gap_s": "Gap to leader (s)"})
    fig.update_yaxes(autorange="reversed")  # leader on top, like a race trace
    return fig


def lap_scatter_fig(df: pd.DataFrame, representative_only: bool = True, fuel_corrected: bool = True):
    d = df[df["is_representative_lap"]] if representative_only else df
    y = "fuel_corrected_lap_time_seconds" if fuel_corrected and d["fuel_corrected_lap_time_seconds"].notna().any() else "lap_time_seconds"
    if d.empty:
        return _empty("No representative laps")
    return px.scatter(d, x="lap_number", y=y, color="tyre_compound", symbol="driver", color_discrete_map=COMPOUND_COLOURS,
                      labels={"lap_number": "Lap", y: "Lap time (s)"})


def stint_gantt_fig(df: pd.DataFrame):
    if df.empty:
        return _empty("No stint data")
    d = df.assign(length=df["last_lap"] - df["first_lap"] + 1)
    fig = go.Figure()
    for comp, g in d.groupby("tyre_compound"):
        fig.add_bar(y=g["driver"], x=g["length"], base=g["first_lap"] - 1, orientation="h", name=str(comp),
                    marker_color=COMPOUND_COLOURS.get(comp, "#888888"),
                    hovertext=[f"stint {s}: laps {a}-{b}" for s, a, b in zip(g["stint_number"], g["first_lap"], g["last_lap"])])
    fig.update_layout(barmode="overlay", xaxis_title="Lap", yaxis={"autorange": "reversed"})
    return fig


def degradation_fig(df: pd.DataFrame):
    d = df[df["degradation_seconds_per_lap"].notna()]
    if d.empty:
        return _empty("Not enough clean laps to estimate degradation")
    return px.bar(d, x="driver", y="degradation_seconds_per_lap", color="tyre_compound", barmode="group",
                  color_discrete_map=COMPOUND_COLOURS, labels={"degradation_seconds_per_lap": "Degradation (s per lap of tyre age)"})


def delta_trace_fig(df: pd.DataFrame):
    if df.empty:
        return _empty("No telemetry delta trace (needs telemetry for the fastest laps)")
    fig = px.line(df, x="distance_m", y="time_delta_seconds", color="driver", labels={"distance_m": "Distance along lap (m)", "time_delta_seconds": "Delta to fastest lap (s)"})
    fig.add_hline(y=0, line_dash="dot")
    return fig


def corner_fig(df: pd.DataFrame):
    d = df[df["driver"].notna()]
    if d.empty:
        return _empty("No corner data")
    return px.bar(d, x="corner_number", y="avg_apex_kph", color="driver", barmode="group", labels={"corner_number": "Detected corner #", "avg_apex_kph": "Apex speed (km/h)"})


def points_fig(df: pd.DataFrame):
    if df.empty:
        return _empty("No points data for this season")
    return px.line(df, x="session_start_time", y="cumulative_points", color="driver", markers=True, labels={"cumulative_points": "Points", "session_start_time": ""})


def sc_hazard_fig(df: pd.DataFrame):
    if df.empty:
        return _empty("No safety car model rows")
    d = df.assign(series=df["circuit_key"].astype(str) + " " + df["track_status"])
    return px.line(d, x="laps_remaining", y="p_at_least_one_start", color="series", markers=True,
                   labels={"laps_remaining": "Laps remaining", "p_at_least_one_start": "P(at least one start)"})


def strategy_fig(rows: list):
    if not rows:
        return _empty("No simulation result")
    df = pd.DataFrame(rows)
    fig = go.Figure()
    fig.add_bar(y=df["strategy"], x=df["mean_s"], orientation="h", name="mean",
                error_x={"type": "data", "symmetric": False, "array": df["p90_s"] - df["mean_s"], "arrayminus": df["mean_s"] - df["p10_s"]})
    fig.update_layout(yaxis={"autorange": "reversed"}, xaxis_title="Total race time (s)  (bars: P10 to P90)")
    lo = float(df["p10_s"].min())
    fig.update_xaxes(range=[lo - 5, float(df["p90_s"].max()) + 5])
    return fig
