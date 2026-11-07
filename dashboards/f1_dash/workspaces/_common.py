import streamlit as st
from .. import state
from ..components import plot


def need_context(con):
    plot.reset()
    ctx = state.current(con)
    if ctx is None:
        st.info("No sessions in the warehouse yet. Run the pipeline and `dbt build` first.")
    return ctx


def need_race(ctx, what: str) -> bool:
    if not ctx.is_race:
        st.info(f"{what} needs a Race or Sprint session. Pick one in the sidebar (current: {ctx.session_name}).")
        return False
    return True
