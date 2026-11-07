"""Race Analyst / Post-race: where was time won and lost, and what was decisive."""
import streamlit as st
from ... import charts, data
from ...components import plot, header, kpis
from ...provenance import panel
from .._common import need_context, need_race


def render(con):
    ctx = need_context(con)
    if ctx is None:
        return
    st.header("Race Analyst: Post-race")
    header.banner(ctx, "Race Analyst", "post")
    if not need_race(ctx, "The post-race review"):
        return
    sk = ctx.session_key
    res = data.results(con, sk)
    fl = data.fastest_laps(con, sk)
    if not res.empty:
        win = res.iloc[0]
        best = res.loc[res["gained"].idxmax()] if res["gained"].notna().any() else None
        kpis.kpi_row([("Winner", win.driver, win.team), ("Fastest lap", f"{fl.iloc[0].driver} {kpis.fmt_lap(fl.iloc[0].fastest_lap_time_seconds)}" if not fl.empty else "-", "session best"),
                      ("Biggest mover", f"{best.driver} {int(best.gained):+d}" if best is not None else "-", "grid to finish"), ("Classified", str(int((res.status.isin(['Finished', 'Lapped'])).sum())), f"of {len(res)}")])
    panel("Classification", "measured", source="fct_race_results")
    st.dataframe(res.dropna(axis=1, how="all"), width="stretch", hide_index=True)     # a race has no Q1-Q3 times
    st.download_button("Export classification (CSV)", res.to_csv(index=False), file_name=f"classification_{sk}.csv", mime="text/csv")

    t1, t2, t3, t4 = st.tabs(["Positions and pace", "Tyres and strategy", "Overtaking and events", "Championship"])
    laps = data.lap_table(con, sk)
    with t1:
        panel("Position progression", "derived", source="fct_race_trace")
        plot.show(charts.position_progression_fig(data.positions_by_lap(con, sk), highlight=[ctx.acronym(ctx.driver)]), st)
        panel("Race trace", "derived", note="Gap to the leader by cumulative lap time.")
        plot.show(charts.race_trace_fig(data.race_trace(con, sk)), st)
        panel("Lap times", "measured", source="fct_lap_performance",
              note="Representative laps exclude first, in, out, yellow, SC/VSC, red laps and slow outliers. Fuel correction is assumption-driven.")
        only_rep = st.checkbox("Representative laps only", value=True, key="ana_post_rep")
        fuel = st.checkbox("Fuel-corrected", value=True, key="ana_post_fuel")
        plot.show(charts.lap_scatter_fig(data.lap_pace(con, sk), only_rep, fuel), st)
        panel("Sector performance", "derived")
        plot.show(charts.sector_heatmap_fig(laps), st)
        panel("Fastest and theoretical best laps", "derived", source="fct_fastest_laps")
        st.dataframe(fl, width="stretch", hide_index=True)
    with t2:
        stints, ps = data.stints(con, sk), data.pit_stops(con, sk)
        panel("Tyre strategy", "measured", source="fct_stints, fct_pit_stops")
        plot.show(charts.stint_gantt_fig(stints, ps), st)
        panel("Degradation per stint", "estimated", note="Regression slope on clean laps; includes fuel, track evolution and traffic.")
        plot.show(charts.degradation_fig(stints), st)
        panel("Pit stops", "measured", note="Pit-lane time is not stationary time; slow stops are flagged relative to the session median.")
        plot.show(charts.pit_duration_fig(ps), st)
        st.dataframe(ps, width="stretch", hide_index=True)
        panel("Undercut / overcut contests", "estimated", source="fct_undercut_overcut")
        uc = data.undercuts(con, sk)
        st.dataframe(uc, width="stretch", hide_index=True) if not uc.empty else st.caption("No related pit-stop pairs with the attacker directly behind the defender.")
    with t3:
        panel("Overtakes", "estimated", source="fct_overtakes", note="'drs_range_proxy' is a gap heuristic, not actual DRS activation.")
        st.dataframe(data.overtakes(con, sk), width="stretch", hide_index=True)
        panel("Neutralisations, penalties and incidents", "measured")
        st.dataframe(data.neutralisations(con, sk), width="stretch", hide_index=True)
        st.dataframe(data.penalties(con, sk), width="stretch", hide_index=True)
        st.dataframe(data.lap_incidents(con, sk), width="stretch", hide_index=True)
    with t4:
        panel(f"{ctx.year} points progression", "derived", source="fct_points_progression", note="Computed from results with the season-aware scoring seed.")
        plot.show(charts.points_fig(data.points_progression(con, ctx.year)), st)
        st.dataframe(data.constructor_standings(con, sk), width="stretch", hide_index=True)
