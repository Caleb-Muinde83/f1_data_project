"""Replay clock: one lap slider that every 'during race' page uses, plus 'jump to event'. Historical replay only, never presented as live."""
import streamlit as st


def _jump(slider_key: str, jump_key: str, lookup_key: str):
    lap = st.session_state.get(lookup_key, {}).get(st.session_state.get(jump_key))
    if lap is not None:
        st.session_state[slider_key] = int(lap)          # the slider follows the chosen event


def replay_controls(total_laps: int, key: str, events=None):
    st.markdown('<span class="f1-tag" style="color:#ff8a3d">replay</span><span class="f1-note"> historical race replayed lap by lap. '
                'Live mode is not available: this project has no streaming feed.</span>', unsafe_allow_html=True)
    total = max(total_laps, 1)
    if total < 2:                              # a slider needs min < max; with 0-1 laps there is nothing to scrub
        st.caption("This session has no lap data in the warehouse (or only one lap), so there is nothing to replay." if total_laps < 1 else "Only one lap: nothing to scrub.")
        return 1
    if key not in st.session_state or not 1 <= st.session_state[key] <= total:
        st.session_state[key] = max(total // 2, 1)
    if events is not None and not events.empty:
        labels = {f"Lap {int(r.lap)}: {r.kind}" + (f" - {r.driver}" if r.driver else "") + f" - {r.detail}": min(int(r.lap), total) for r in events.itertuples()}
        st.session_state[f"{key}_events"] = labels
        st.selectbox("Jump to event", ["(choose an event)"] + list(labels), key=f"{key}_jump",
                     on_change=_jump, args=(key, f"{key}_jump", f"{key}_events"))
    return st.slider("Race lap", 1, total, key=key)
