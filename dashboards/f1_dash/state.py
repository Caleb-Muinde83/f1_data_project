"""Shared race context: ONE season / Grand Prix / session / driver selection that every workspace reads."""
from dataclasses import dataclass
from typing import Optional
import pandas as pd
import streamlit as st
from . import data


@dataclass
class Context:
    session_key: int
    meeting_key: int
    year: int
    circuit_key: Optional[int]
    session_name: str
    session_type: str
    start_time: pd.Timestamp
    target_key: int                 # the Race/Sprint the pre-race view prepares for (== session_key when a race is selected)
    target_start: pd.Timestamp      # information cut-off for pre-race analysis
    drivers: pd.DataFrame
    driver: Optional[int]           # focus driver_number
    rival: Optional[int]            # comparison driver_number

    @property
    def is_race(self) -> bool:
        return self.session_type in ("Race", "Sprint")

    def acronym(self, driver_number) -> str:
        m = self.drivers[self.drivers.driver_number == driver_number]
        return m.iloc[0].driver_acronym if not m.empty else str(driver_number)


def _pick(options, key, default):
    cur = st.session_state.get(key)
    return cur if cur in options else default


def current(con) -> Optional[Context]:
    """Build the Context from st.session_state (widgets live in the sidebar, see components.filters). Falls back to sensible
    defaults, so a workspace page also renders on its own (tests, deep links)."""
    m = data.meetings(con)
    if m.empty:
        return None
    years = sorted(m.year.unique(), reverse=True)
    year = _pick(years, "ctx_year", years[0])
    mm = m[m.year == year]
    meeting = int(_pick(list(mm.meeting_key), "ctx_meeting", mm.iloc[0].meeting_key))
    sess = data.meeting_sessions(con, meeting)
    races = sess[sess.session_type.isin(["Race", "Sprint"])]
    default_sk = int((races if not races.empty else sess).iloc[-1].session_key)
    sk = int(_pick(list(sess.session_key), "ctx_session", default_sk))
    row = sess[sess.session_key == sk].iloc[0]
    target = races[races.session_type == "Race"] if (races.session_type == "Race").any() else races
    if row.session_type in ("Race", "Sprint") or target.empty:
        target_key, target_start = sk, row.session_start_time
    else:
        target_key, target_start = int(target.iloc[-1].session_key), target.iloc[-1].session_start_time
    drv = data.drivers(con, sk)
    nums = list(drv.driver_number)
    driver = _pick(nums, "ctx_driver", nums[0] if nums else None)
    rival = _pick([n for n in nums if n != driver], "ctx_rival", next((n for n in nums if n != driver), None))
    return Context(sk, meeting, int(year), None if pd.isna(row.circuit_key) else int(row.circuit_key), row.session_name, row.session_type,
                   row.session_start_time, target_key, target_start, drv, driver, rival)
