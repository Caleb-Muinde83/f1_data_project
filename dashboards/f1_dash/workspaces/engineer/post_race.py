"""Race Engineer / Post-race: debrief and technical investigation."""
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header, telemetry
from ...provenance import panel
from .._common import need_context


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Engineer: Post-race")
    header.banner(ctx, "Race Engineer", "post")
    sk = ctx.session_key
    (a, la), (b, lb) = telemetry.overlay_inputs(con, sk, ctx.driver, ctx.rival, ctx.drivers)
    t1, t2, t3, t4 = st.tabs(["Telemetry debrief", "Corners and circuit", "Tyres and stints", "Reliability and incidents"])
    with t1:
        panel("Two-lap overlay (fastest laps)", "measured", source="fct_telemetry_distance_trace")
        plot.show(charts.telemetry_overlay_fig(a, b, la, lb), st)
        telemetry.note_missing(a, b, la, lb)
        panel("Delta to the fastest lap along the lap", "derived", source="fct_lap_time_delta_trace")
        plot.show(charts.delta_trace_fig(data.delta_trace(con, sk)), st)
        panel("Lap-time ceiling", "derived", source="fct_fastest_laps", note="Theoretical best combines each driver's best three sectors; it is not always achievable.")
        st.dataframe(data.fastest_laps(con, sk), width="stretch", hide_index=True)
    with t2:
        panel("Circuit map coloured by speed", "measured", source="focus driver's fastest lap")
        corners_xy = data.q(con, "SELECT corner_number, apex_x, apex_y FROM dim_session_corners WHERE session_key = ?", [sk])
        plot.show(charts.circuit_map_fig(telemetry.reference_trace(con, sk, ctx.driver), corners_xy), st)
        panel("Apex speed by detected corner", "estimated", note="Corners are detected from telemetry; numbers are not official turn numbers.")
        cn = data.corners(con, sk)
        plot.show(charts.corner_fig(cn), st)
        st.dataframe(cn, width="stretch", hide_index=True)
    with t3:
        panel("Degradation per stint", "estimated", source="fct_stints", note="Regression slope on clean laps: includes fuel, track evolution and traffic effects, not tyre wear alone.")
        stints = data.stints(con, sk)
        plot.show(charts.degradation_fig(stints), st)
        st.dataframe(stints, width="stretch", hide_index=True)
    with t4:
        laps = data.lap_table(con, sk)
        panel("Unusual laps (statistical screen)", "estimated", note="Robust z-score per driver. A flag means 'look at this lap', not 'something broke'.")
        sc = insights.robust_anomalies(laps[laps.driver_number == ctx.driver]) if ctx.driver is not None else laps.head(0)
        plot.show(charts.anomaly_fig(sc), st)
        panel("Penalties and neutralisations", "measured", source="fct_penalties, fct_safety_car_events")
        st.dataframe(data.penalties(con, sk), width="stretch", hide_index=True)
        st.dataframe(data.neutralisations(con, sk), width="stretch", hide_index=True)
        header.not_available("Mechanical failures, temperature excursions, root cause", "No sensor or incident-investigation data source. Record findings in your team's system.")
