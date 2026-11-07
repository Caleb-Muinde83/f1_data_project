"""Race Analyst / During race (REPLAY): timing tower, gaps, pace and events up to the chosen lap."""
import streamlit as st
from ... import charts, data
from ...components import plot, header, kpis, race_timeline, timing
from ...provenance import panel
from .._common import need_context, need_race


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Analyst: During race (replay)")
    header.banner(ctx, "Race Analyst", "during")
    if not need_race(ctx, "The race replay"):
        return
    total = data.session_laps(con, ctx.session_key)
    lap = race_timeline.replay_controls(total, key="ana_dur_lap")
    laps = data.lap_table(con, ctx.session_key)
    order = data.race_order(con, ctx.session_key)
    upto = laps[laps.lap_number <= lap]
    now = order[order.lap_number == lap].sort_values("position").merge(laps[laps.lap_number == lap][["driver_number", "driver"]], on="driver_number")
    leader = now.iloc[0] if not now.empty else None
    me = now[now.driver_number == ctx.driver]
    pos = f"P{int(me.iloc[0].position)}" if not me.empty else "-"
    kpis.kpi_row([("Lap", f"{lap}/{total}", "replay clock"), ("Leader", leader.driver if leader is not None else "-", ""),
                  ("Focus driver", pos, ctx.acronym(ctx.driver)),
                  ("Gap ahead", kpis.fmt_gap(me.iloc[0].interval) if not me.empty else "-", "interval at lap end"),
                  ("Track", ((laps[laps.lap_number == lap].track_status.dropna().mode().iloc[0]) if laps[laps.lap_number == lap].track_status.notna().any() else "-"), "race control")])

    panel("Timing tower", "measured", source="fct_lap_performance", note="Position and gaps come from cumulative race time, so they always agree with the position chart below.")
    timing.show_tower(laps, lap, ctx.driver, order)

    c1, c2 = st.columns(2)
    names = [ctx.acronym(ctx.driver), ctx.acronym(ctx.rival)]
    with c1:
        panel("Position progression", "derived", source="fct_race_trace")
        pos = data.positions_by_lap(con, ctx.session_key)
        plot.show(charts.position_progression_fig(pos[pos.lap_number <= lap], highlight=names), st)
    with c2:
        panel("Gap to the car ahead", "derived", source="fct_race_trace (cumulative time)",
              note="The shaded band is a DRS-range proxy (within 1.0 s), not actual DRS activation.")
        gaps = order.merge(laps[["driver_number", "driver", "lap_number"]], on=["driver_number", "lap_number"])
        gaps = gaps[gaps.lap_number <= lap].rename(columns={"interval": "gap_to_car_ahead_end_seconds"})
        plot.show(charts.gap_evolution_fig(gaps, names), st)
    panel("Pace and rolling average", "derived", source="fct_lap_performance")
    plot.show(charts.pace_rolling_fig(upto, names), st)

    c3, c4 = st.columns(2)
    with c3:
        panel("Pit stops so far", "measured", source="fct_pit_stops", note="Pit-lane time, not stationary time.")
        ps = data.pit_stops(con, ctx.session_key)
        st.dataframe(ps[ps.lap_number <= lap], width="stretch", hide_index=True)
    with c4:
        panel("Overtakes so far", "estimated", source="fct_overtakes", note="DRS column is a gap proxy, not DRS activation.")
        ov = data.overtakes(con, ctx.session_key)
        st.dataframe(ov[ov.lap_number <= lap], width="stretch", hide_index=True)
    panel("Race events so far", "measured", source="fct_safety_car_events, fct_penalties")
    nz = data.neutralisations(con, ctx.session_key)
    st.dataframe(nz[nz.start_lap <= lap], width="stretch", hide_index=True)
    pn = data.penalties(con, ctx.session_key)
    st.dataframe(pn[pn.lap_number <= lap], width="stretch", hide_index=True)
