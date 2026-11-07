"""Race Engineer / Pre-race: is the car and driver ready? Uses ONLY earlier sessions of this weekend."""
import streamlit as st
from ... import charts, data
from ...components import plot, header, telemetry
from ...provenance import panel
from .._common import need_context


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Engineer: Pre-race")
    header.banner(ctx, "Race Engineer", "pre")
    prior = data.prior_sessions(con, ctx.meeting_key, ctx.target_start)
    st.caption(f"Information cut-off {ctx.target_start}: only sessions of this weekend that started earlier are used.")
    base_sk = None
    if prior.empty:
        st.info("No earlier session of this weekend in the warehouse, so there is no baseline to review.")
    else:
        labels = {int(r.session_key): f"{r.session_name} ({r.session_key})" for r in prior.itertuples()}
        base_sk = st.selectbox("Baseline session", list(labels), format_func=labels.get, index=len(labels) - 1, key="eng_pre_base")

    t_base, t_circuit, t_tyres, t_weather, t_setup = st.tabs(["Telemetry baseline", "Circuit", "Tyres", "Weather", "Setup and run plan"])
    with t_base:
        if base_sk is None:
            st.caption("No baseline: this weekend has no earlier session in the warehouse.")
        else:
            (a, la), (b, lb) = telemetry.overlay_inputs(con, base_sk, ctx.driver, ctx.rival, ctx.drivers)
            panel("Fastest-lap telemetry overlay", "measured", source="fct_telemetry_distance_trace",
                  note="Distance is integrated from speed and normalised: it is not an official track-distance measurement.")
            plot.show(charts.telemetry_overlay_fig(a, b, la, lb), st)
            telemetry.note_missing(a, b, la, lb)
            panel("Per-lap telemetry summary", "derived", source="fct_telemetry_lap_summary")
            ts = data.telemetry_lap_summary(con, base_sk)
            c1, c2 = st.columns(2)
            plot.show(charts.lap_summary_fig(ts, "top_speed_kph", "Top speed (km/h)"), c1)
            plot.show(charts.lap_summary_fig(ts, "full_throttle_fraction", "Full-throttle fraction"), c2)
    with t_circuit:
        if base_sk is None:
            st.caption("No baseline: this weekend has no earlier session in the warehouse.")
        else:
            panel("Circuit map coloured by speed", "measured", source="GPS + speed of the focus driver's fastest lap")
            plot.show(charts.circuit_map_fig(telemetry.reference_trace(con, base_sk, ctx.driver), _corners_xy(con, base_sk)), st)
            panel("Detected corners", "estimated", source="dim_session_corners / fct_corner_performance",
                  note="Corners are DETECTED from telemetry (OpenF1 provides no corner definitions): numbers are not official turn numbers.")
            cn = data.corners(con, base_sk)
            plot.show(charts.corner_fig(cn), st)
            st.dataframe(cn, width="stretch", hide_index=True)
    with t_tyres:
        panel("Tyre behaviour in earlier sessions", "estimated", source="fct_compound_pace_model",
              note="A straight-line fit of fuel-corrected lap time against tyre age. It cannot separate tyre wear from track evolution or traffic.")
        cp = data.compound_pace(con, list(prior.session_key)) if not prior.empty else None
        if cp is None or cp.empty:
            st.info("No fitted compound models in earlier sessions (needs enough clean laps per compound).")
        else:
            st.dataframe(cp, width="stretch", hide_index=True)
        header.not_available("Tyre temperatures, pressures, graining and blistering", "OpenF1 exposes tyre compound and age only, not per-wheel temperatures or pressures.")
    with t_weather:
        panel("Weather and track temperature", "measured", source="fct_weather_impact (per lap)")
        w = data.weather_by_lap(con, list(prior.session_key)) if not prior.empty else None
        if w is None or w.empty:
            st.info("No weather observations in earlier sessions.")
        else:
            import plotly.express as px
            plot.show(px.line(w, x="lap_number", y="track_temp", color="session_name", labels={"track_temp": "Track temp (C)", "lap_number": "Lap"}), st)
        header.not_available("Forecast and rain probability", "No forecast feed is connected; only observed conditions are available.")
    with t_setup:
        for title, why in [("Car setup (wings, ride height, anti-roll bars, differential, brake balance)", "Setup sheets are team-private and not in the OpenF1 API."),
                           ("Power unit and energy management", "Battery state of charge, deployment and component life are not published."),
                           ("Engineering run plan and setup history", "No source system; the platform would need a manual or team-system integration.")]:
            header.not_available(title, why)
        panel("Engineering notes", "manual", note="Kept in this browser session only: nothing is saved to the warehouse.")
        st.text_area("Notes", key="eng_notes", label_visibility="collapsed", placeholder="Questions to resolve before the race...")


def _corners_xy(con, sk):
    return data.q(con, "SELECT corner_number, apex_x, apex_y FROM dim_session_corners WHERE session_key = ?", [sk])
