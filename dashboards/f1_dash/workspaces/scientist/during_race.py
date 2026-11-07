"""Race Data Scientist / During race (REPLAY): re-estimate with only the laps seen so far."""
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header, kpis, race_timeline
from ...provenance import panel
from .._common import need_context, need_race


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Data Scientist: During race (replay)")
    header.banner(ctx, "Race Data Scientist", "during")
    if not need_race(ctx, "The race replay"):
        return
    total = data.session_laps(con, ctx.session_key)
    lap = race_timeline.replay_controls(total, key="ds_dur_lap")
    laps = data.lap_table(con, ctx.session_key)
    upto = laps[laps.lap_number <= lap]

    panel("Remaining stint pace projection", "predicted", source="line fitted on the current stint's clean laps up to this lap (replay-safe)",
          note="A straight-line extrapolation with a ~90% residual interval. It will miss a tyre cliff.")
    fit = insights.fit_stint_degradation(laps, ctx.driver, lap, horizon=min(15, max(total - lap, 1)))
    if fit:
        n_loss = insights.laps_until_loss(fit, 1.0)
        kpis.kpi_row([("Compound", fit["compound"], f"{fit['n']} clean laps in fit"), ("Degradation", f"{fit['slope']:+.3f} s/lap", "fitted slope"),
                      ("Residual sigma", f"{fit['sigma']:.2f} s", "lap-to-lap scatter"), ("Laps to +1.0 s", str(n_loss) if n_loss else "n/a", "from fitted start pace")])
    plot.show(charts.degradation_projection_fig(fit), st)

    c1, c2 = st.columns(2)
    with c1:
        panel("Lap-time anomaly screen", "estimated", note="Robust z-score on laps seen so far. Flags laps to inspect; it does not say why.")
        an = insights.robust_anomalies(upto[upto.driver_number == ctx.driver]) if ctx.driver is not None else upto.head(0)
        plot.show(charts.anomaly_fig(an), st)
    with c2:
        remaining = max(total - lap, 0)
        panel("Safety car hazard with laps remaining", "estimated", source="fct_safety_car_hazard")
        hz = data.sc_hazard(con, ctx.circuit_key) if ctx.circuit_key is not None else data.sc_hazard(con)
        if not hz.empty:
            near = hz.iloc[(hz["laps_remaining"] - remaining).abs().argsort()[:1]]
            st.caption(f"{remaining} laps remain; nearest modelled horizon: {int(near.iloc[0].laps_remaining)} laps.")
        plot.show(charts.sc_hazard_fig(hz), st)
    header.not_available("Live re-simulation, undercut probability and 'pit now vs later' expected gain",
                        "Needs a rival model and a live position feed. The Monte Carlo simulator runs on the Pre-race and Post-race pages.")
