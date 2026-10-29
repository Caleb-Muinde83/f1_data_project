"""Run:  F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb streamlit run dashboards/app.py"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))                 # f1_dash
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "analytics"))  # f1_strategy
import streamlit as st
from f1_dash import data, pages

st.set_page_config(page_title="F1 Analytics", layout="wide")


@st.cache_resource
def _con():
    return data.connect()


try:
    con = _con()
except Exception as exc:
    st.error(str(exc)); st.stop()

nav = st.navigation([
    st.Page(lambda: pages.overview(con), title="Race overview", url_path="overview", default=True),
    st.Page(lambda: pages.pace(con), title="Pace and tyres", url_path="pace"),
    st.Page(lambda: pages.strategy(con), title="Strategy", url_path="strategy"),
    st.Page(lambda: pages.telemetry(con), title="Telemetry", url_path="telemetry"),
    st.Page(lambda: pages.safety_car(con), title="Safety car risk", url_path="safety-car"),
    st.Page(lambda: pages.radio(con), title="Team radio", url_path="radio"),
])
nav.run()
