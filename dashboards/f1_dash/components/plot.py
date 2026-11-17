"""Every chart goes through show(): it gives each Plotly chart a unique key.
Streamlit derives a chart's id from its content, so two identical charts (for example two 'no data' placeholders) on one page
raise StreamlitDuplicateElementId unless each has its own key."""
import plotly.graph_objects as go
import streamlit as st


def to_svg(fig):
    """Plotly Express switches to WebGL (scattergl) above ~1000 rows, which can fail to draw lines in some browsers/screenshot tools (a 20-driver, 56-lap
    race is 1,120 rows). Convert every scattergl trace to plain SVG scatter: slower only beyond many thousands of points, and always visible."""
    d = fig.to_dict()
    changed = False
    for t in d.get("data", []):
        if t.get("type") == "scattergl":
            t["type"] = "scatter"
            changed = True
    return go.Figure(d) if changed else fig


def reset():
    st.session_state["_plot_n"] = 0


def show(fig, container=None):
    n = st.session_state.get("_plot_n", 0) + 1
    st.session_state["_plot_n"] = n
    return (container or st).plotly_chart(to_svg(fig), width="stretch", key=f"plot-{n}")
