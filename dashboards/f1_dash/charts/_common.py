import zlib

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


# One fixed colour per driver, so a driver looks the same on every chart (plotly assigns colours by order of appearance, which changed
# a driver's colour from chart to chart and repeated colours when more than ten drivers were drawn). 26 colours, readable on the dark theme.
_PALETTE = ["#e10600", "#3da5ff", "#ffd12e", "#39b54a", "#b983ff", "#ff8c42", "#2ee6d6", "#ff6fb5", "#d9d9d9", "#8aa0b8", "#c0392b", "#2980b9", "#f1c40f",
            "#27ae60", "#9b59b6", "#e67e22", "#1abc9c", "#fd79a8", "#a29bfe", "#74b9ff", "#55efc4", "#fab1a0", "#dfe6e9", "#ff7675", "#00cec9", "#b2bec3"]
_DRIVER_ORDER = ["ANT", "RUS", "HAM", "LEC", "NOR", "PIA", "VER", "HAD", "ALB", "SAI", "ALO", "STR", "GAS", "COL", "OCO", "BEA", "LAW", "LIN", "HUL", "BOR", "BOT", "PER"]
DRIVER_COLOURS = {a: _PALETTE[i] for i, a in enumerate(_DRIVER_ORDER)}


def driver_colour(acronym) -> str:
    """Fixed colour for a driver acronym. Drivers outside the 2026 grid get a stable colour from a hash of the acronym."""
    a = str(acronym)
    return DRIVER_COLOURS.get(a) or _PALETTE[zlib.crc32(a.encode()) % len(_PALETTE)]


def driver_colour_map(names) -> dict:
    """{acronym: colour} for every driver in `names`, for plotly's color_discrete_map."""
    return {str(n): driver_colour(n) for n in names if n is not None and str(n) != "nan"}
