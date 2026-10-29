"""Build simulator inputs from the dbt marts. Every value that is a FALLBACK (not measured) is recorded in provenance."""
from statistics import median
from .model import CompoundModel, RaceContext


def load_inputs(con, session_key: int, driver_number: int = None, total_laps: int = None, overrides: dict = None):
    prov, overrides = {}, overrides or {}
    sess = con.execute("SELECT circuit_key FROM dim_sessions WHERE session_key = ?", [session_key]).fetchone()
    if sess is None:
        raise ValueError(f"session {session_key} not found in dim_sessions")
    circuit = sess[0]

    if total_laps is None:
        row = con.execute("SELECT MAX(lap_number) FROM int_lap_context WHERE session_key = ?", [session_key]).fetchone()
        total_laps = int(row[0]) if row and row[0] else None
        prov["total_laps"] = "measured: max lap in session" if total_laps else "MISSING"
    if not total_laps:
        raise ValueError("total_laps could not be determined; pass total_laps explicitly")

    rows = con.execute(
        "SELECT driver_number, tyre_compound, fresh_tyre_pace_seconds, degradation_seconds_per_lap, clean_laps "
        "FROM fct_compound_pace_model WHERE session_key = ? AND fresh_tyre_pace_seconds IS NOT NULL "
        "AND degradation_seconds_per_lap IS NOT NULL", [session_key]).fetchall()
    models, by_compound = {}, {}
    for d, comp, pace, deg, n in rows:
        by_compound.setdefault(comp, []).append((d, pace, deg, n))
    for comp, items in by_compound.items():
        mine = [i for i in items if driver_number is not None and i[0] == driver_number]
        use, how = (mine, f"driver {driver_number}") if mine else (items, f"median of {len(items)} driver(s)")
        pace = median(i[1] for i in use); deg = median(i[2] for i in use)
        if deg < 0:
            prov[f"{comp}.degradation"] = f"clipped from {deg:.4f} to 0 (negative slope = noise or track evolution)"
            deg = 0.0
        models[comp] = CompoundModel(float(pace), float(deg))
        prov.setdefault(f"{comp}.model", f"measured: {how}")

    pl = con.execute("SELECT median_green_time_lost_seconds, median_green_pit_lane_time_seconds, neutralisation_discount_ratio "
                     "FROM fct_pit_loss_by_circuit WHERE circuit_key = ?", [circuit]).fetchone()
    pit_loss = pl[0] if pl and pl[0] is not None else (pl[1] if pl and pl[1] is not None else None)
    prov["pit_loss"] = "measured: circuit median" if pit_loss is not None else "FALLBACK default 22.0 s"
    discount = pl[2] if pl and pl[2] is not None else None
    prov["neutralised_pit_discount"] = "measured" if discount is not None else "FALLBACK default 0.5 (no neutralised stops observed)"

    sc = con.execute("SELECT shrunk_hazard_per_lap, avg_duration_laps, low_sample FROM fct_safety_car_hazard "
                     "WHERE circuit_key = ? AND track_status = 'SC' AND laps_remaining = 5", [circuit]).fetchone()
    hazard = sc[0] if sc and sc[0] is not None else 0.0
    duration = sc[1] if sc and sc[1] is not None else 3.0
    prov["sc_hazard"] = ("measured, LOW SAMPLE (shrunk toward all-circuit rate)" if sc and sc[2] else "measured") if sc else "FALLBACK 0 (no data)"
    prov["sc_duration"] = "measured" if sc and sc[1] is not None else "FALLBACK 3.0 laps"

    ctx = RaceContext(
        total_laps=total_laps,
        pit_loss=float(overrides.get("pit_loss", pit_loss if pit_loss is not None else 22.0)),
        neutralised_pit_discount=float(overrides.get("neutralised_pit_discount", discount if discount is not None else 0.5)),
        sc_hazard_per_lap=float(overrides.get("sc_hazard_per_lap", hazard)),
        sc_duration_laps=float(overrides.get("sc_duration_laps", duration)),
        **{k: v for k, v in overrides.items() if k in ("fuel_effect", "sc_lap_time_factor", "noise_sd", "start_penalty")},
    )
    return ctx, models, prov
