"""Timing tower for one lap of a (replayed) race."""
import pandas as pd
import streamlit as st
from .kpis import fmt_lap


def tower(laps: pd.DataFrame, lap: int, focus: int = None, order: pd.DataFrame = None) -> pd.DataFrame:
    """Timing tower. When `order` (data.race_order) is given, Pos/gaps come from cumulative race time, which cannot produce two cars in one position."""
    d = laps[laps["lap_number"] == lap]
    if order is not None and not order.empty:
        o = order[order["lap_number"] == lap][["driver_number", "position", "gap_to_leader", "interval"]]
        d = d.drop(columns=["position_end", "gap_to_leader_end_seconds", "gap_to_car_ahead_end_seconds"]).merge(o, on="driver_number", how="inner")
        d = d.rename(columns={"position": "position_end", "gap_to_leader": "gap_to_leader_end_seconds", "interval": "gap_to_car_ahead_end_seconds"})
    d = d.sort_values("position_end")
    best = laps[laps["lap_number"] <= lap].groupby("driver_number")["lap_time_seconds"].min()
    t = pd.DataFrame({
        "Pos": d["position_end"].astype("Int64"), "Driver": d["driver"], "Team": d["team"],
        "Gap to leader": d["gap_to_leader_end_seconds"].map(lambda v: "-" if v != v else f"+{v:.1f}"),
        "Interval": d["gap_to_car_ahead_end_seconds"].map(lambda v: "-" if v != v else f"+{v:.1f}"),
        "Last lap": d["lap_time_seconds"].map(fmt_lap), "Best": d["driver_number"].map(best).map(fmt_lap),
        "Tyre": d["tyre_compound"], "Age": d["tyre_age_this_lap"].astype("Int64"), "Track": d["track_status"]})
    return t.reset_index(drop=True)


def show_tower(laps, lap, focus=None, order=None):
    t = tower(laps, lap, focus, order)
    st.dataframe(t, width="stretch", hide_index=True)
