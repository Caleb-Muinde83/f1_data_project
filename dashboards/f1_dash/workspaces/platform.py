"""Platform / Data coverage: what the warehouse holds, what every number means, and what is NOT here."""
import pandas as pd
import streamlit as st
from .. import data, provenance
from ..components import header

LIMITS = [
    ("fct_compound_pace_model", "A straight-line regression cannot separate tyre degradation from fuel, track evolution and traffic."),
    ("fct_overtakes", "The DRS classification is a gap proxy, not actual DRS activation."),
    ("fct_safety_car_hazard", "A smoothed historical rate is not a calibrated live prediction."),
    ("fct_pit_stops", "Pit-lane duration is not stationary pit-stop time."),
    ("fct_telemetry_distance_trace", "Distance is integrated from speed and normalised; it is not an official track-distance measurement."),
    ("dim_session_corners", "Detected braking / speed zones are not official corner numbers."),
]

REGISTRY = [
    ("ML-01", "Lap-time prediction", "Partial", "dbt compound regression (fresh pace + degradation x tyre age)"),
    ("ML-02", "Sector-time prediction", "Not implemented", ""),
    ("ML-03", "Tyre degradation", "Baseline", "fct_compound_pace_model; replay re-fit on the dashboard"),
    ("ML-04", "Tyre life estimation", "Baseline", "laps until +1.0 s on the fitted line (During race, Data Scientist)"),
    ("ML-05", "Finishing-position prediction", "Baseline", "grid-slot base rate from earlier races"),
    ("ML-06", "Pit-stop prediction", "Not implemented", ""),
    ("ML-07", "Overtaking probability", "Not implemented", "fct_overtakes holds the labels"),
    ("ML-08", "Safety-car modelling", "Baseline", "fct_safety_car_hazard (shrunk empirical rate)"),
    ("ML-09", "Telemetry anomaly detection", "Baseline", "robust z-score on lap times (not telemetry channels)"),
    ("ML-10", "Strategy optimiser", "Implemented (limited)", "analytics/f1_strategy; ignores traffic and rival reactions"),
    ("ML-11", "Race outcome simulator", "Partial", "single-car Monte Carlo of total race time, not a field simulation"),
    ("ML-12", "Weather-adjusted performance", "Not implemented", ""),
]


def render(con):
    st.header("Data coverage and provenance")
    st.markdown("### How to read a number")
    provenance.legend()
    st.markdown("### Warehouse contents")
    st.caption(f"File: {data.CURRENT_DB}" + ("  (SYNTHETIC CI fixtures)" if data.is_fixture_warehouse(con) else ""))
    cov = data.data_coverage(con)
    st.dataframe(cov, width="stretch", hide_index=True)
    st.markdown("### Not in this dataset")
    for t, why in [("Live streaming", "The dashboards read a DuckDB warehouse. Replay pages are historical; nothing here is a live feed."),
                   ("Car setup, sensors, tyre temperatures and pressures", "Not published by OpenF1."),
                   ("Weather forecast and rain probability", "Only observed conditions."),
                   ("Official corner definitions, DRS zones, marshal sectors", "Corners are detected from telemetry.")]:
        header.not_available(t, why)
    st.markdown("### Known limitations of derived models")
    st.dataframe(pd.DataFrame(LIMITS, columns=["Model", "Limitation"]), width="stretch", hide_index=True)
    st.markdown("### ML model registry")
    st.dataframe(pd.DataFrame(REGISTRY, columns=["ID", "Model", "Status", "Where / notes"]), width="stretch", hide_index=True)
