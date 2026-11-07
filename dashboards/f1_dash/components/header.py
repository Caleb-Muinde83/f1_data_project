"""Context banner shown at the top of every workspace page. Historical data is never presented as live."""
import streamlit as st

PHASES = {
    "pre": ("PRE-RACE", "Historical information available before lights out; model predictions."),
    "during": ("RACE REPLAY", "Historical race replayed lap by lap. NOT a live feed."),
    "post": ("POST-RACE", "Final results, explanations and model evaluation."),
}


def banner(ctx, role: str, phase: str, as_of: str = ""):
    label, hint = PHASES[phase]
    extra = f" &middot; as of {as_of}" if as_of else ""
    st.markdown(
        f'<div class="f1-banner"><b>{role.upper()}</b><span>&middot;</span><b style="color:#e10600">{label}</b><span>&middot;</span>'
        f'<span>{ctx.year} {ctx.session_name} (session {ctx.session_key}){extra}</span>'
        f'<span class="f1-note">&nbsp;{hint} Source: DuckDB warehouse, status: completed.</span></div>', unsafe_allow_html=True)


def not_available(title: str, why: str):
    """Honest placeholder for something the OpenF1-based warehouse does not contain."""
    st.markdown(f'<div class="f1-alert"><b>{title}</b> &mdash; not available. {why}</div>', unsafe_allow_html=True)
