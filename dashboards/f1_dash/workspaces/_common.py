import streamlit as st
from .. import state
from ..components import plot


def need_context(con):
    plot.reset()
    ctx = state.current(con)
    if ctx is None:
        st.info("No sessions in the warehouse yet. Run the pipeline and `dbt build` first.")
    return ctx


def need_laps(total_laps: int, session_name: str) -> bool:
    """A session can exist in dim_sessions with no lap rows (extract missing, or a session that was never run)."""
    if total_laps < 1:
        st.warning(f"No lap data for {session_name} in the warehouse, so the replay and lap-based panels are empty. Check the extract for this session (Data coverage).")
        return False
    return True


def need_race(ctx, what: str) -> bool:
    if not ctx.is_race:
        st.info(f"{what} needs a Race or Sprint session. Pick one in the sidebar (current: {ctx.session_name}).")
        return False
    return True
