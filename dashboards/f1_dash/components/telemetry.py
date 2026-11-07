"""Helpers that fetch the two-lap telemetry overlay data (each driver's fastest lap by default)."""
import pandas as pd
import streamlit as st
from .. import data


def fastest_lap_number(con, session_key: int, driver_number: int):
    f = data.fastest_laps(con, session_key)
    m = f[f["driver_number"] == driver_number]
    return None if m.empty else int(m.iloc[0]["lap_number"])


def overlay_inputs(con, session_key: int, driver: int, rival: int, drivers: pd.DataFrame):
    """Returns ((df_a, label_a), (df_b, label_b)) with each driver's fastest lap in `session_key`."""
    def acr(n):
        m = drivers[drivers.driver_number == n]
        return m.iloc[0].driver_acronym if not m.empty else str(n)
    la = fastest_lap_number(con, session_key, driver) if driver is not None else None
    lb = fastest_lap_number(con, session_key, rival) if rival is not None else None
    a = data.distance_trace(con, session_key, driver, la) if la else pd.DataFrame()
    b = data.distance_trace(con, session_key, rival, lb) if lb else pd.DataFrame()
    return (a, f"{acr(driver)} L{la}"), (b, f"{acr(rival)} L{lb}")


def reference_trace(con, session_key: int, driver: int):
    """GPS + speed of the driver's fastest lap: the base of the circuit map."""
    lap = fastest_lap_number(con, session_key, driver) if driver is not None else None
    return data.distance_trace(con, session_key, driver, lap) if lap else pd.DataFrame()


def note_missing(a, b, label_a, label_b):
    """The overlay silently shows one trace when a driver has no telemetry; say so."""
    for df, lbl in ((a, label_a), (b, label_b)):
        if df.empty:
            st.caption(f"No telemetry for {lbl.split(' L')[0]} in this session (telemetry is only stored for some drivers/laps), so only one trace is shown.")
