"""Sidebar selectors that fill the shared context (season > Grand Prix > session > drivers)."""
import streamlit as st
from .. import data


def sidebar(con):
    m = data.meetings(con)
    with st.sidebar:
        if data.is_fixture_warehouse(con):
            st.warning("SYNTHETIC TEST DATA: this is the CI fixture warehouse, not your races. Unset F1_DUCKDB_PATH or point it at your real warehouse.")
        st.markdown("### Race context")
        if m.empty:
            st.info("No sessions in the warehouse yet.")
            return
        year = st.selectbox("Season", sorted(m.year.unique(), reverse=True), key="ctx_year")
        mm = m[m.year == year]
        names = {int(r.meeting_key): f"{r.location} ({r.meeting_name})" for r in mm.itertuples()}
        meeting = st.selectbox("Grand Prix", list(names), format_func=names.get, key="ctx_meeting")
        sess = data.meeting_sessions(con, meeting)
        labels = {int(r.session_key): f"{r.session_name}" for r in sess.itertuples()}
        sk = st.selectbox("Session", list(labels), format_func=labels.get, index=len(labels) - 1, key="ctx_session")
        drv = data.drivers(con, sk)
        nums = list(drv.driver_number)
        fmt = {int(r.driver_number): f"{r.driver_acronym} - {r.team_name}" for r in drv.itertuples()}
        if nums:
            st.selectbox("Focus driver", nums, format_func=fmt.get, key="ctx_driver")
            others = [n for n in nums if n != st.session_state.get("ctx_driver")]
            if others:
                st.selectbox("Compare with", others, format_func=fmt.get, key="ctx_rival")
        st.caption("These selections carry across every workspace.")
        st.caption(f"Warehouse: {data.CURRENT_DB}")
