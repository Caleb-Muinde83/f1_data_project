"""Every analytical panel says WHAT KIND of number it shows: measured, derived, estimated, predicted, simulated, historical, manual."""
import streamlit as st

KINDS = {
    "measured":  ("#39b54a", "Directly measured by the timing / telemetry feed"),
    "derived":   ("#3da5ff", "Calculated from measured data (rules, joins, aggregation)"),
    "estimated": ("#ffd12e", "Estimated: a proxy or fitted value, not a measurement"),
    "predicted": ("#b983ff", "Predicted by a statistical model"),
    "simulated": ("#ff8a3d", "Output of a Monte Carlo simulation"),
    "historical": ("#8b93a1", "Taken from earlier sessions or seasons"),
    "manual":    ("#2ee6d6", "Entered by a person"),
}


def tag(kind: str) -> str:
    colour, title = KINDS[kind]
    return f'<span class="f1-tag" style="color:{colour}" title="{title}">{kind}</span>'


def panel(title: str, *kinds: str, source: str = "", note: str = "", level: int = 3):
    """Section heading with provenance tags, the data source and an optional caveat."""
    st.markdown(f"{'#' * level} {title}")
    bits = "".join(tag(k) for k in kinds)
    src = f' <span class="f1-note">source: {source}</span>' if source else ""
    st.markdown(f"{bits}{src}", unsafe_allow_html=True)
    if note:
        st.markdown(f'<div class="f1-note">{note}</div>', unsafe_allow_html=True)


def legend():
    for k, (colour, title) in KINDS.items():
        st.markdown(f"{tag(k)} {title}", unsafe_allow_html=True)
