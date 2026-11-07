"""F1 Performance Intelligence.
Run:  F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb streamlit run dashboards/app.py

Navigation is organised by professional role (Race Engineer / Race Analyst / Race Data Scientist) and race phase
(Pre-race / During race replay / Post-race). One shared context (season > Grand Prix > session > drivers) lives in the sidebar."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))                 # f1_dash
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "analytics"))  # f1_strategy
import streamlit as st
from f1_dash import data, theme
from f1_dash.components import filters
from f1_dash.workspaces import platform
from f1_dash.workspaces.engineer import pre_race as eng_pre, during_race as eng_dur, post_race as eng_post
from f1_dash.workspaces.analyst import pre_race as ana_pre, during_race as ana_dur, post_race as ana_post
from f1_dash.workspaces.scientist import pre_race as ds_pre, during_race as ds_dur, post_race as ds_post

st.set_page_config(page_title="F1 Performance Intelligence", page_icon=":checkered_flag:", layout="wide")
theme.apply()


@st.cache_resource
def _con():
    return data.connect()


try:
    con = _con()
except Exception as exc:
    st.error(str(exc)); st.stop()

def run_con():
    """A fresh cursor per script run: one shared DuckDB connection raises 'connection is busy' when reruns overlap."""
    return con.cursor()


filters.sidebar(run_con())


def page(module, title, path, **kw):
    return st.Page(lambda: module.render(run_con()), title=title, url_path=path, **kw)


nav = st.navigation({
    "Race Engineer": [page(eng_pre, "Pre-race", "engineer-pre", icon=":material/build:", default=True),
                      page(eng_dur, "During race (replay)", "engineer-during", icon=":material/speed:"),
                      page(eng_post, "Post-race", "engineer-post", icon=":material/fact_check:")],
    "Race Analyst": [page(ana_pre, "Pre-race", "analyst-pre", icon=":material/query_stats:"),
                     page(ana_dur, "During race (replay)", "analyst-during", icon=":material/leaderboard:"),
                     page(ana_post, "Post-race", "analyst-post", icon=":material/summarize:")],
    "Race Data Scientist": [page(ds_pre, "Pre-race", "scientist-pre", icon=":material/model_training:"),
                            page(ds_dur, "During race (replay)", "scientist-during", icon=":material/monitoring:"),
                            page(ds_post, "Post-race", "scientist-post", icon=":material/rule:")],
    "Platform": [page(platform, "Data coverage", "coverage", icon=":material/database:")],
})
nav.run()
