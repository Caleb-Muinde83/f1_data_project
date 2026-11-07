import streamlit as st


def kpi_row(items):
    """items: list of (label, value, subtext)."""
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        col.markdown(f'<div class="f1-kpi"><div class="l">{label}</div><div class="v">{value}</div><div class="s">{sub}</div></div>',
                     unsafe_allow_html=True)


def fmt_lap(seconds) -> str:
    if seconds is None or seconds != seconds:
        return "-"
    m, s = divmod(float(seconds), 60)
    return f"{int(m)}:{s:06.3f}" if m else f"{s:.3f}"


def fmt_gap(v) -> str:
    return "-" if v is None or v != v else f"+{float(v):.1f}s"
