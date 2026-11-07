"""Dark graphite motorsport look: one CSS block for Streamlit and one Plotly template for every chart."""
import plotly.graph_objects as go
import plotly.io as pio

BG, PANEL, EDGE, TEXT, MUTED = "#0e1013", "#171a1f", "#2a2f37", "#e8eaed", "#8b93a1"
ACCENT = "#e10600"
PALETTE = ["#e10600", "#3da5ff", "#ffd12e", "#39b54a", "#b983ff", "#ff8a3d", "#2ee6d6", "#ff6fb1", "#c7ccd4", "#7a8699"]

_TEMPLATE = go.layout.Template(layout=go.Layout(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=PANEL, font={"family": "Inter, Segoe UI, sans-serif", "color": TEXT, "size": 12},
    colorway=PALETTE, margin={"l": 50, "r": 20, "t": 40, "b": 45},
    xaxis={"gridcolor": EDGE, "zerolinecolor": EDGE, "linecolor": EDGE}, yaxis={"gridcolor": EDGE, "zerolinecolor": EDGE, "linecolor": EDGE},
    legend={"bgcolor": "rgba(0,0,0,0)"}, hoverlabel={"bgcolor": PANEL, "bordercolor": EDGE}))
pio.templates["f1_dark"] = _TEMPLATE
pio.templates.default = "f1_dark"

CSS = f"""
<style>
.block-container {{ padding-top: 1.4rem; max-width: 1500px; }}
h1, h2, h3 {{ letter-spacing: .01em; }}
.f1-banner {{ display:flex; gap:.6rem; align-items:center; flex-wrap:wrap; background:{PANEL}; border:1px solid {EDGE};
  border-left:4px solid {ACCENT}; border-radius:6px; padding:.55rem .9rem; margin:.2rem 0 1rem 0; font-size:.9rem; }}
.f1-banner b {{ letter-spacing:.06em; }}
.f1-kpi {{ background:{PANEL}; border:1px solid {EDGE}; border-radius:6px; padding:.6rem .9rem; }}
.f1-kpi .l {{ color:{MUTED}; font-size:.72rem; text-transform:uppercase; letter-spacing:.08em; }}
.f1-kpi .v {{ font-size:1.6rem; font-weight:700; line-height:1.2; }}
.f1-kpi .s {{ color:{MUTED}; font-size:.75rem; }}
.f1-tag {{ display:inline-block; padding:1px 8px; border-radius:10px; font-size:.68rem; font-weight:600; letter-spacing:.05em;
  text-transform:uppercase; border:1px solid currentColor; margin-right:.4rem; vertical-align:middle; }}
.f1-note {{ color:{MUTED}; font-size:.8rem; }}
.f1-alert {{ border-left:4px solid #ffd12e; background:{PANEL}; padding:.4rem .8rem; border-radius:4px; margin:.25rem 0; font-size:.88rem; }}
.f1-alert.crit {{ border-left-color:{ACCENT}; }}
</style>
"""


def apply():
    import streamlit as st
    st.markdown(CSS, unsafe_allow_html=True)
