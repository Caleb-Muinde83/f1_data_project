"""Race Data Scientist / Pre-race: what is likely to happen? Simple, transparent baselines built ONLY from earlier data."""
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header, kpis, simulator
from ...provenance import panel
from .._common import need_context


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Data Scientist: Pre-race")
    header.banner(ctx, "Race Data Scientist", "pre")
    st.caption(f"Training cut-off {ctx.target_start}: models see only races and sessions that started earlier (no leakage).")

    panel("Finishing-position base rate", "predicted", source="grid-vs-finish history of earlier races (baseline model ML-05)",
          note="Not a pace model: it only says how cars starting near this grid slot have finished before. Treat small samples with care.")
    grid = data.starting_grid(con, ctx.target_key)
    g = grid[grid.driver_number == ctx.driver] if not grid.empty else grid
    if g.empty:
        st.info("No starting grid for the focus driver in the target race.")
    else:
        slot = int(g.iloc[0].grid)
        window = st.slider("Grid window (+- places)", 0, 3, 1, key="ds_pre_win")
        dist = insights.grid_baseline(data.grid_finish_history(con, ctx.target_start), slot, window)
        if dist.empty:
            st.info("No earlier races in the warehouse to build a base rate from.")
        else:
            n = int(dist.n.iloc[0])
            kpis.kpi_row([("Grid", f"P{slot}", ctx.acronym(ctx.driver)), ("Expected finish", f"{insights.expected_finish(dist):.1f}", "classified cars only"),
                          ("P(podium)", f"{insights.p_at_most(dist, 3):.0%}", f"n = {n}"), ("P(points)", f"{insights.p_at_most(dist, 10):.0%}", f"n = {n}"),
                          ("P(DNF/unclassified)", f"{float(dist.loc[dist.finish == insights.DNF_BUCKET, 'probability'].iloc[0]):.0%}", f"n = {n}")])
            if n < 30:
                st.markdown(f'<div class="f1-alert">Only {n} earlier starts match this window: probabilities are very uncertain.</div>', unsafe_allow_html=True)
            plot.show(charts.finishing_distribution_fig(dist, slot), st)

    c1, c2 = st.columns(2)
    with c1:
        panel("Safety car hazard at this circuit", "estimated", source="fct_safety_car_hazard",
              note="Smoothed historical rate, not a calibrated live prediction. Check `low_sample`.")
        hz = data.sc_hazard(con, ctx.circuit_key) if ctx.circuit_key is not None else data.sc_hazard(con)
        plot.show(charts.sc_hazard_fig(hz), st)
    with c2:
        prior = data.prior_sessions(con, ctx.meeting_key, ctx.target_start)
        panel("Tyre degradation models from earlier sessions", "predicted", source="fct_compound_pace_model (ML-03 baseline)",
              note="Linear in tyre age; cannot separate tyre wear from track evolution, fuel and traffic.")
        cp = data.compound_pace(con, list(prior.session_key)) if not prior.empty else None
        st.dataframe(cp, width="stretch", hide_index=True) if cp is not None and not cp.empty else st.caption("No fitted compound models in earlier sessions.")

    if not prior.empty and ctx.is_race:
        labels = {int(r.session_key): f"{r.session_name} ({r.session_key})" for r in prior.itertuples()}
        calib = st.selectbox("Calibrate the simulator from", list(labels), format_func=labels.get, index=len(labels) - 1, key="ds_pre_calib")
        laps = st.number_input("Race distance (laps)", 10, 100, 57, key="ds_pre_laps", help="Pre-race the distance is an input, not a measurement.")
        simulator.render(con, calib, total_laps=int(laps), driver=ctx.driver, key="ds_pre_sim",
                         warning="Caution: pit loss and safety-car hazard come from marts that include EVERY race in the warehouse, possibly this one. "
                                 "Use the sliders; a true backtest needs marts rebuilt with a cutoff.")
    else:
        st.caption("The strategy simulator needs an earlier session to calibrate from and a Race/Sprint target.")
