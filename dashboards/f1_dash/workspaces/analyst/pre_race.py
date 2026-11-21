"""Race Analyst / Pre-race: who is fast, where, and what happened here before. Uses ONLY information from before the race."""
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header
from ...provenance import panel
from .._common import need_context


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Analyst: Pre-race")
    header.banner(ctx, "Race Analyst", "pre")
    st.caption(f"Information cut-off {ctx.target_start}.")
    prior = data.prior_sessions(con, ctx.meeting_key, ctx.target_start)
    c1, c2 = st.columns(2)
    with c1:
        panel("Starting grid", "measured", source="fct_race_results", note="Known before lights out.")
        grid = data.starting_grid(con, ctx.target_key)
        if grid.empty:
            st.info("No grid positions for this race in the warehouse (see Data coverage > Data health).")
        else:
            st.dataframe(grid.drop(columns=["driver_number"]), width="stretch", hide_index=True)
    with c2:
        panel("Championship going into the race", "derived", source="fct_points_progression",
              note="Computed from results of sessions that started before the cut-off. That includes this weekend's Sprint, if there is one.")
        pts = data.points_before(con, ctx.year, ctx.target_start)
        st.dataframe(pts, width="stretch", hide_index=True) if not pts.empty else st.caption("No earlier scored session this season.")

    panel("Previous races at this circuit", "historical", source="fct_race_results, dim_sessions")
    if ctx.circuit_key is None:
        st.caption("Circuit unknown for this session.")
    else:
        hist = data.prior_races_at_circuit(con, ctx.circuit_key, ctx.target_start)
        st.dataframe(hist, width="stretch", hide_index=True) if not hist.empty else st.caption("No earlier race at this circuit in the warehouse.")
        sc = data.sc_events_at_circuit(con, ctx.circuit_key, ctx.target_start)
        panel("Neutralisations seen here before", "historical", source="fct_safety_car_events")
        st.dataframe(sc, width="stretch", hide_index=True) if not sc.empty else st.caption("None recorded (or no earlier race here).")

    if prior.empty:
        st.info("No earlier session this weekend, so there is no competitor pace to compare yet.")
        return
    labels = {int(r.session_key): f"{r.session_name} ({r.session_key})" for r in prior.itertuples()}
    sk = st.selectbox("Earlier session to analyse", list(labels), format_func=labels.get, index=len(labels) - 1, key="ana_pre_sess")
    q = data.qualifying_results(con, [sk])
    if not q.empty:
        panel("Qualifying classification", "measured", source="fct_race_results (Qualifying)")
        st.dataframe(q, width="stretch", hide_index=True)
    panel("Best laps and gap to the fastest", "measured", source="fct_fastest_laps")
    fl = data.fastest_laps(con, sk)
    st.dataframe(fl, width="stretch", hide_index=True)
    panel("Sector strengths", "derived", note="Best clean sector per driver relative to the session best. Drivers without a lap within 107% of the best are hidden; colours clip at 1.5 s.")
    plot.show(charts.sector_heatmap_fig(data.lap_table(con, sk)), st)
    panel("Long-run tyre pace", "estimated", source="fct_compound_pace_model")
    cp = data.compound_pace(con, [sk])
    if cp.empty:
        st.caption("Not enough clean laps per compound to fit a pace model.")
    else:
        good = cp[insights.usable_pace_fits(cp)]
        st.caption(f"{len(cp) - len(good)} of {len(cp)} fits hidden as unusable (too few clean laps, pace far off the session's best, or an implausible slope).")
        st.dataframe(good.drop(columns=["session_key"]), width="stretch", hide_index=True) if not good.empty else st.caption("No usable fits in this session.")
