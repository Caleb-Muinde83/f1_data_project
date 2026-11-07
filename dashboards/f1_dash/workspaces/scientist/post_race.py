"""Race Data Scientist / Post-race: how good were the models, and could a different strategy have helped?"""
import pandas as pd
import streamlit as st
from ... import charts, data, insights
from ...components import plot, header, kpis, simulator
from ...provenance import panel
from .._common import need_context, need_race


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Data Scientist: Post-race")
    header.banner(ctx, "Race Data Scientist", "post")
    if not need_race(ctx, "Model evaluation"):
        return
    sk = ctx.session_key
    t1, t2, t3, t4 = st.tabs(["Finishing-position model", "Lap-time model", "Safety car", "Strategy counterfactual"])
    with t1:
        panel("Predicted vs actual finishing position", "predicted", source="grid base rate trained on earlier races only (ML-05 baseline)")
        grid, res = data.starting_grid(con, sk), data.results(con, sk)
        hist = data.grid_finish_history(con, ctx.start_time)
        rows = []
        for r in grid.itertuples():
            dist = insights.grid_baseline(hist, int(r.grid), 1)
            act = res.loc[res.driver == r.driver, "pos"]
            if not dist.empty and not act.empty and act.iloc[0] == act.iloc[0]:
                rows.append({"driver": r.driver, "predicted": insights.expected_finish(dist), "actual": float(act.iloc[0]), "n": int(dist.n.iloc[0])})
        ev = pd.DataFrame(rows)
        if ev.empty:
            st.info("No earlier races to train the baseline on, so there is nothing to evaluate yet.")
        else:
            m = insights.error_metrics(ev.actual, ev.predicted)
            kpis.kpi_row([("MAE", f"{m['mae']:.2f}", "places"), ("RMSE", f"{m['rmse']:.2f}", "places"), ("Bias", f"{m['bias']:+.2f}", "actual - predicted"), ("Drivers", str(m["n"]), "evaluated")])
            plot.show(charts.predicted_vs_actual_fig(ev, "predicted", "actual", "Predicted finish", "Actual finish"), st)
            st.caption("Predictions use only races that started before this one, but a handful of races is not enough to claim a model is good or bad.")
    with t2:
        panel("Lap-time model residuals", "derived", source="fct_compound_pace_model vs fct_lap_performance",
              note="IN-SAMPLE: the dbt model is fitted on this same race, so errors here are optimistic. Use it to find where a straight line fails.")
        laps = data.lap_table(con, sk)
        model = data.compound_pace(con, [sk])
        resid = insights.pace_model_residuals(laps, model)
        if resid.empty:
            st.info("No fitted compound models for this session.")
        else:
            m = insights.error_metrics(resid.actual, resid.predicted)
            kpis.kpi_row([("MAE", f"{m['mae']:.3f} s", "in-sample"), ("RMSE", f"{m['rmse']:.3f} s", ""), ("Bias", f"{m['bias']:+.3f} s", "actual - predicted"), ("Laps", str(m["n"]), "representative")])
            plot.show(charts.residual_fig(resid), st)
            st.dataframe(resid.groupby("driver").residual.agg(mae=lambda s: s.abs().mean(), bias="mean", laps="count").round(4).reset_index(), width="stretch", hide_index=True)
    with t3:
        panel("Observed neutralisations vs the hazard model", "historical", source="fct_safety_car_events, fct_safety_car_hazard",
              note="Calibrating a rare-event model needs many races. Here we only show what happened next to the model's curve.")
        st.dataframe(data.neutralisations(con, sk), width="stretch", hide_index=True)
        plot.show(charts.sc_hazard_fig(data.sc_hazard(con, ctx.circuit_key) if ctx.circuit_key is not None else data.sc_hazard(con)), st)
    with t4:
        simulator.render(con, sk, driver=ctx.driver, key="ds_post_sim",
                         warning="In-sample counterfactual: the simulator is calibrated on THIS race's own laps, so it shows what the model thinks of alternatives, not what would have happened.")
        header.not_available("Strategy regret against rivals, SHAP / feature importance, drift monitoring",
                            "No multivariate model is trained in this project yet (see the model registry on the Data coverage page).")
