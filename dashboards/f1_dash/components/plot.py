"""Every chart goes through show(): it gives each Plotly chart a unique key.
Streamlit derives a chart's id from its content, so two identical charts (for example two 'no data' placeholders) on one page
raise StreamlitDuplicateElementId unless each has its own key."""
import streamlit as st


def reset():
    st.session_state["_plot_n"] = 0


def show(fig, container=None):
    n = st.session_state.get("_plot_n", 0) + 1
    st.session_state["_plot_n"] = n
    return (container or st).plotly_chart(fig, width="stretch", key=f"plot-{n}")
