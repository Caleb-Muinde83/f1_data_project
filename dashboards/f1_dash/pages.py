"""One render function per dashboard page. Each takes an open DuckDB connection and draws with Streamlit."""
import streamlit as st
from . import charts, data


def _pick_session(con, race_only=False, key="session"):
    s = data.sessions(con, race_only)
    if s.empty:
        st.info("No sessions in the warehouse yet. Run the pipeline and `dbt build` first.")
        return None
    options = {data.label(r): int(r.session_key) for r in s.itertuples()}
    return options[st.selectbox("Session", list(options), key=key)]


def overview(con):
    st.header("Race overview")
    sk = _pick_session(con, race_only=True, key="ov_session")
    if sk is None:
        return
    st.subheader("Classification")
    st.dataframe(data.results(con, sk), width="stretch", hide_index=True)
    st.subheader("Race trace (gap to leader by cumulative lap time)")
    st.plotly_chart(charts.race_trace_fig(data.race_trace(con, sk)), width="stretch")
    year = int(data.q(con, "SELECT year FROM dim_sessions WHERE session_key = ?", [sk]).iloc[0, 0])
    st.subheader(f"{year} points progression (computed from race results)")
    st.plotly_chart(charts.points_fig(data.points_progression(con, year)), width="stretch")


def pace(con):
    st.header("Pace and tyres")
    sk = _pick_session(con, key="pace_session")
    if sk is None:
        return
    only_rep = st.checkbox("Representative laps only", value=True, help="Excludes first/in/out/yellow/SC/VSC/red laps and slow outliers")
    fuel = st.checkbox("Fuel-corrected (assumption-driven)", value=True)
    st.plotly_chart(charts.lap_scatter_fig(data.lap_pace(con, sk), only_rep, fuel), width="stretch")
    st.subheader("Tyre degradation per stint")
    st.plotly_chart(charts.degradation_fig(data.stints(con, sk)), width="stretch")
    st.dataframe(data.stints(con, sk), width="stretch", hide_index=True)


def strategy(con):
    st.header("Strategy")
    sk = _pick_session(con, race_only=True, key="strat_session")
    if sk is None:
        return
    st.subheader("Tyre strategy")
    st.plotly_chart(charts.stint_gantt_fig(data.stints(con, sk)), width="stretch")
    st.subheader("Pit stops")
    st.dataframe(data.pit_stops(con, sk), width="stretch", hide_index=True)
    st.subheader("Undercut / overcut contests")
    uc = data.undercuts(con, sk)
    if uc.empty:
        st.caption("No related pit-stop pairs with the attacker directly behind the defender.")
    else:
        st.dataframe(uc, width="stretch", hide_index=True)
    simulator(con, sk)


def simulator(con, sk: int):
    st.subheader("Strategy simulator (Monte Carlo)")
    st.caption("Decision support, not a forecast: it ignores traffic, tyre cliffs, track evolution and rival reactions, and is "
               "not validated against real races. Inputs come from the dbt marts; fallbacks are listed below.")
    try:
        from f1_strategy import load_inputs, enumerate_strategies, rank_strategies
        ctx, models, prov = load_inputs(con, sk)
    except ImportError:
        st.warning("The f1_strategy package is not on the Python path (see dashboards/README).")
        return
    except Exception as exc:
        st.warning(f"Cannot build simulator inputs for this session: {exc}")
        return
    if len(models) < 2:
        st.warning(f"Need at least two compounds with a fitted pace model; found {sorted(models)}.")
        return
    c1, c2, c3 = st.columns(3)
    hazard = c1.slider("Safety car chance per lap", 0.0, 0.10, float(ctx.sc_hazard_per_lap), 0.005)
    pit_loss = c2.slider("Pit loss (s)", 15.0, 35.0, float(ctx.pit_loss), 0.5)
    sims = c3.select_slider("Simulations", [500, 1000, 2000, 4000], value=1000)
    from dataclasses import replace
    ctx = replace(ctx, sc_hazard_per_lap=hazard, pit_loss=pit_loss)
    strategies = enumerate_strategies(ctx.total_laps, sorted(models), max_stops=2, pit_step=max(1, ctx.total_laps // 20))
    rows = rank_strategies(ctx, models, strategies, n_sims=sims, top=8)
    st.plotly_chart(charts.strategy_fig(rows), width="stretch")
    st.dataframe(rows, width="stretch", hide_index=True)
    with st.expander("Where the inputs came from"):
        st.json(prov)


def telemetry(con):
    st.header("Telemetry")
    sk = _pick_session(con, key="tel_session")
    if sk is None:
        return
    st.subheader("Delta to the fastest lap along the lap")
    st.plotly_chart(charts.delta_trace_fig(data.delta_trace(con, sk)), width="stretch")
    st.subheader("Detected corners")
    st.caption("Corners are DETECTED from telemetry (the API provides none): corner numbers are not official turn numbers.")
    cn = data.corners(con, sk)
    st.plotly_chart(charts.corner_fig(cn), width="stretch")
    st.dataframe(cn, width="stretch", hide_index=True)


def safety_car(con):
    st.header("Safety car risk")
    st.caption("Empirical estimate shrunk toward the all-circuit rate; per-circuit evidence is thin (see low_sample).")
    st.plotly_chart(charts.sc_hazard_fig(data.sc_hazard(con)), width="stretch")
    st.dataframe(data.sc_hazard(con), width="stretch", hide_index=True)


def radio(con):
    st.header("Team radio")
    sk = _pick_session(con, race_only=True, key="radio_session")
    if sk is None:
        return
    df = data.radio(con, sk)
    if df.empty:
        st.info("No transcripts. Enable `transcribe_radio` in the DAG and `enable_radio_transcripts` in dbt.")
        return
    tags = [c for c in df.columns if c.startswith("mentions_")]
    chosen = st.multiselect("Show messages tagged", tags)
    if chosen:
        df = df[df[chosen].any(axis=1)]
    st.caption("Tags are keyword heuristics on speech-to-text output: use them to find messages, not as ground truth.")
    st.dataframe(df, width="stretch", hide_index=True)
