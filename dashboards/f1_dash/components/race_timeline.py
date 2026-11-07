"""Replay clock: one lap slider that every 'during race' page uses. Historical replay only, never presented as live."""
import streamlit as st


def replay_controls(total_laps: int, key: str):
    st.markdown('<span class="f1-tag" style="color:#ff8a3d">replay</span><span class="f1-note"> historical race replayed lap by lap. '
                'Live mode is not available: this project has no streaming feed.</span>', unsafe_allow_html=True)
    return st.slider("Race lap", 1, max(total_laps, 1), max(total_laps, 1) if total_laps < 2 else max(total_laps // 2, 1), key=key)
