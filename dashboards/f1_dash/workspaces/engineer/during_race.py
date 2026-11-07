"""Race Engineer / During race (REPLAY): what is the car doing? Reads historical data lap by lap."""
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header, kpis, race_timeline
from ...provenance import panel
from .._common import need_context, need_race


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Engineer: During race (replay)")
    header.banner(ctx, "Race Engineer", "during")
    if not need_race(ctx, "The race replay"):
        return
    total = data.session_laps(con, ctx.session_key)
    lap = race_timeline.replay_controls(total, key="eng_dur_lap")
    laps = data.lap_table(con, ctx.session_key)
    me = laps[(laps.driver_number == ctx.driver) & (laps.lap_number == lap)]
    ts = data.telemetry_lap_summary(con, ctx.session_key)
    t_me = ts.head(0) if ts.empty else ts[(ts.driver_number == ctx.driver) & (ts.lap_number == lap)]
    r = me.iloc[0] if not me.empty else None
    kpis.kpi_row([
        ("Driver", ctx.acronym(ctx.driver), f"lap {lap}/{total}"),
        ("Last lap", kpis.fmt_lap(r.lap_time_seconds) if r is not None else "-", f"P{int(r.position_end)}" if r is not None and r.position_end == r.position_end else ""),
        ("Tyre", f"{r.tyre_compound} / {int(r.tyre_age_this_lap)} laps" if r is not None and r.tyre_age_this_lap == r.tyre_age_this_lap else "-", "compound / age"),
        ("Track status", (r.track_status if r is not None else "-") or "-", "from race control"),
        ("Top speed", f"{t_me.iloc[0].top_speed_kph:.0f} km/h" if not t_me.empty else "-", "this lap, telemetry")])

    left, right = st.columns([3, 2])
    with left:
        panel("Telemetry summary by lap", "derived", source="fct_telemetry_lap_summary", note="Samples are aggregated per lap; the replay does not stream raw channels.")
        win = ts if ts.empty else ts[(ts.lap_number <= lap) & ts.driver_number.isin([ctx.driver, ctx.rival])]
        plot.show(charts.lap_summary_fig(win, "top_speed_kph", "Top speed (km/h)"), st)
        plot.show(charts.lap_summary_fig(win, "braking_fraction", "Fraction of lap on the brakes"), st)
    with right:
        panel("Engineering alerts", "derived", note="Rule-based screens on timing, penalty and radio data. There are no car sensors in this dataset.")
        radio = data.radio(con, ctx.session_key)
        alerts = insights.engineering_alerts(laps, data.penalties(con, ctx.session_key), radio, ctx.driver, lap)
        if not alerts:
            st.caption("No alerts for the focus driver up to this lap.")
        for sev, l, msg in alerts[-8:]:
            st.markdown(f'<div class="f1-alert {"crit" if sev == "crit" else ""}">{msg}</div>', unsafe_allow_html=True)
        panel("Team radio so far", "measured", source="fct_radio_messages (speech-to-text)")
        if radio.empty:
            st.caption("No transcripts. Enable `transcribe_radio` in the DAG and `enable_radio_transcripts` in dbt.")
        else:
            mine = radio[(radio.lap_number <= lap) & (radio.driver_name.notna())]
            st.dataframe(mine[["lap_number", "driver_name", "transcript"]].tail(8), width="stretch", hide_index=True)

    panel("Track status and incidents so far", "measured", source="fct_lap_incidents")
    inc = data.lap_incidents(con, ctx.session_key)
    st.dataframe(inc[inc.lap_number <= lap], width="stretch", hide_index=True)
    header.not_available("Live car health (engine, oil, brake, hydraulic, battery) and per-wheel tyre temperatures", "Not published by OpenF1. These panels need a team telemetry feed.")
