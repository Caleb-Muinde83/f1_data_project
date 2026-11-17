"""Driver picker: 20 overlapping lines or bars are unreadable, so charts default to the focus and comparison drivers."""
import streamlit as st


def drivers(ctx, df, key: str, col: str = "driver"):
    options = sorted(df[col].dropna().unique()) if df is not None and not df.empty and col in df else []
    default = [a for a in (ctx.acronym(ctx.driver), ctx.acronym(ctx.rival)) if a in options] or options[:2]
    return st.multiselect("Drivers to show", options, default=default, key=key) if options else []
