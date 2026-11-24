"""Monte Carlo strategy simulator panel (analytics/f1_strategy). Decision support, not a forecast."""
from dataclasses import replace
import streamlit as st
from .. import charts
from . import plot
from ..provenance import panel


def clipped_compounds(prov: dict):
    """Compounds whose fitted degradation was clipped to 0 because the slope was negative (fuel burn and track evolution hide wear in short stints)."""
    return sorted(k.split(".")[0] for k, v in (prov or {}).items() if k.endswith(".degradation") and "clipped" in str(v))


def viable(con, session_keys, driver=None, total_laps=None):
    """Sessions the simulator can really be calibrated from: at least two compounds with a fitted pace model (the same test render() applies)."""
    try:
        from f1_strategy import load_inputs
    except ImportError:
        return []
    ok = []
    for sk in session_keys:
        try:
            _, models, _ = load_inputs(con, int(sk), driver_number=driver, total_laps=total_laps)
            if len(models) >= 2:
                ok.append(int(sk))
        except Exception:
            continue
    return ok


def ranking_is_meaningless(clipped, models) -> bool:
    """True when no compound has a measured wear rate: every fitted slope was clipped to 0, so the ranking only reflects the pace gap between compounds and
    pit timing (it puts the short stint on the slower compound), not anything about tyre life."""
    return bool(clipped) and set(clipped) >= set(models)


def render(con, calib_session: int, total_laps: int = None, driver: int = None, warning: str = "", key: str = "sim"):
    panel("Strategy simulator (Monte Carlo)", "simulated", source="analytics/f1_strategy, calibrated from dbt marts",
          note="Decision support, not a forecast: it ignores traffic, tyre cliffs, track evolution and rival reactions, and is not validated against real races.")
    if warning:
        st.markdown(f'<div class="f1-alert">{warning}</div>', unsafe_allow_html=True)
    try:
        from f1_strategy import load_inputs, enumerate_strategies, rank_strategies
        ctx, models, prov = load_inputs(con, calib_session, driver_number=driver, total_laps=total_laps)
    except ImportError:
        st.warning("The f1_strategy package is not on the Python path (see dashboards/README)."); return
    except Exception as exc:
        st.warning(f"Cannot build simulator inputs from session {calib_session}: {exc}"); return
    if len(models) < 2:
        st.warning(f"Session {calib_session} has a fitted pace model for only {sorted(models)}. The simulator needs two compounds: pick a session where both were run "
                   f"(an earlier race at this circuit is usually best)."); return
    clipped = clipped_compounds(prov)
    if clipped:
        st.markdown(f'<div class="f1-alert crit">Tyre degradation could not be estimated for {", ".join(clipped)} (the fitted slope was negative and was clipped to 0). '
                    f'With no wear modelled, the ranking below reflects only the pace difference between compounds and the pit timing: it says nothing about when the tyres wear out. '
                    f'Calibrate from a longer, cleaner session if you can.</div>', unsafe_allow_html=True)
        if ranking_is_meaningless(clipped, models):
            with st.expander("Where the inputs came from"):
                st.json(prov)
            if not st.checkbox("Show the ranking anyway (pace gap and pit timing only, no tyre wear)", value=False, key=f"{key}_anyway"):
                st.caption("The ranking is hidden because no compound has a measured wear rate: any order it gave would come from the pace gap alone.")
                return
    c1, c2, c3 = st.columns(3)
    hazard = c1.slider("Safety car chance per lap", 0.0, 0.10, float(min(ctx.sc_hazard_per_lap, 0.10)), 0.005, key=f"{key}_sc")
    pit_loss = c2.slider("Pit loss (s)", 15.0, 35.0, float(min(max(ctx.pit_loss, 15.0), 35.0)), 0.5, key=f"{key}_pit")
    sims = c3.select_slider("Simulations", [500, 1000, 2000, 4000], value=1000, key=f"{key}_n")
    ctx = replace(ctx, sc_hazard_per_lap=hazard, pit_loss=pit_loss)
    strategies = enumerate_strategies(ctx.total_laps, sorted(models), max_stops=2, pit_step=max(1, ctx.total_laps // 20))
    rows = rank_strategies(ctx, models, strategies, n_sims=sims, top=8)
    plot.show(charts.strategy_fig(rows), st)
    st.dataframe(rows, width="stretch", hide_index=True)
    with st.expander("Where the inputs came from"):
        st.json(prov)
