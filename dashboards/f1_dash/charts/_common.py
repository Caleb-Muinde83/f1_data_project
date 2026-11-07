import plotly.graph_objects as go

COMPOUND_COLOURS = {"SOFT": "#e10600", "MEDIUM": "#ffd12e", "HARD": "#dddddd", "INTERMEDIATE": "#39b54a", "WET": "#1e6fd9", "UNKNOWN": "#888888"}


def empty(msg: str):
    """A figure that says why there is nothing to draw (never a crash, never a blank chart)."""
    fig = go.Figure()
    fig.add_annotation(text=msg, showarrow=False, font={"size": 15})
    fig.update_xaxes(visible=False); fig.update_yaxes(visible=False)
    fig.update_layout(height=200)          # a message needs no tall empty plot
    return fig


def lacks(df, *cols) -> bool:
    """True when the frame is empty or is missing a column the chart needs (e.g. a column-less empty DataFrame)."""
    return df is None or df.empty or any(c not in df.columns for c in cols)
