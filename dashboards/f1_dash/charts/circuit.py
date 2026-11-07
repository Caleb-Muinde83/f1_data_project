"""Circuit map built from GPS samples of one reference lap."""
import pandas as pd
import plotly.graph_objects as go
from ._common import empty, lacks


def circuit_map_fig(trace: pd.DataFrame, corners: pd.DataFrame = None, colour_by: str = "speed_kph"):
    if lacks(trace, "x", "y", colour_by) or trace["x"].isna().all():
        return empty("No GPS trace for this session")
    fig = go.Figure(go.Scatter(x=trace["x"], y=trace["y"], mode="markers+lines", line={"color": "#2a2f37", "width": 8},
                               marker={"size": 7, "color": trace[colour_by], "colorscale": "Turbo", "showscale": True, "colorbar": {"title": "km/h" if colour_by == "speed_kph" else colour_by}},
                               customdata=trace[["distance_m", colour_by]], hovertemplate="%{customdata[0]:.0f} m<br>%{customdata[1]:.0f}<extra></extra>", showlegend=False))
    if corners is not None and not corners.empty and corners["apex_x"].notna().any():
        fig.add_scatter(x=corners["apex_x"], y=corners["apex_y"], mode="markers+text", text=corners["corner_number"].astype(int).astype(str),
                        textposition="top center", marker={"size": 11, "color": "#fff", "symbol": "circle-open"}, name="detected corner")
    fig.update_yaxes(scaleanchor="x", visible=False); fig.update_xaxes(visible=False)
    fig.update_layout(height=520, legend={"orientation": "h", "yanchor": "bottom", "y": -0.05, "x": 0})
    return fig
