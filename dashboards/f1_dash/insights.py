"""Small, dependency-light analytics used by the workspaces (pure pandas/numpy, unit tested).

Honesty rules: every model here is a simple, transparent BASELINE and the pages label it as such. Functions only use the rows
they are given, so callers are responsible for passing data that existed at prediction time (no leakage).
"""
import numpy as np
import pandas as pd

DNF_BUCKET = 21


def grid_baseline(history: pd.DataFrame, grid: int, window: int = 1) -> pd.DataFrame:
    """Empirical finishing-position distribution for cars that started within +-window of `grid` in EARLIER races.
    Retirements and unclassified cars go to bucket 21. Returns columns finish, probability, n (sample size)."""
    if history is None or history.empty or grid is None:
        return pd.DataFrame(columns=["finish", "probability", "n"])
    h = history[(history["grid_position"] - grid).abs() <= window]
    if h.empty:
        return pd.DataFrame(columns=["finish", "probability", "n"])
    fin = h["final_position"].where(h["classification_status"].isin(["Finished", "Lapped"]) & h["final_position"].notna(), DNF_BUCKET).astype(int)
    counts = fin.value_counts().reindex(range(1, DNF_BUCKET + 1), fill_value=0)
    return pd.DataFrame({"finish": counts.index, "probability": counts.values / counts.sum(), "n": int(counts.sum())})


def expected_finish(dist: pd.DataFrame) -> float:
    """Mean finishing position over CLASSIFIED outcomes only (DNFs are reported separately)."""
    d = dist[dist["finish"] < DNF_BUCKET]
    return float((d["finish"] * d["probability"]).sum() / d["probability"].sum()) if not d.empty and d["probability"].sum() > 0 else float("nan")


def p_at_most(dist: pd.DataFrame, position: int) -> float:
    return float(dist.loc[dist["finish"] <= position, "probability"].sum()) if not dist.empty else float("nan")


def fit_stint_degradation(laps: pd.DataFrame, driver_number: int, upto_lap: int, horizon: int = 10, min_points: int = 4):
    """Linear lap-time-vs-tyre-age fit on the driver's CURRENT stint using only laps <= upto_lap (replay-safe).
    Returns a dict for charts.degradation_projection_fig, or None when there are too few clean laps."""
    d = laps[(laps["driver_number"] == driver_number) & (laps["lap_number"] <= upto_lap)]
    if d.empty:
        return None
    d = d[d["stint_number"] == d.sort_values("lap_number").iloc[-1]["stint_number"]]
    y = d["fuel_corrected_lap_time_seconds"].where(d["fuel_corrected_lap_time_seconds"].notna(), d["lap_time_seconds"])
    ok = d["is_representative_lap"] & y.notna() & d["tyre_age_this_lap"].notna()
    d, y = d[ok], y[ok]
    if len(d) < min_points or d["tyre_age_this_lap"].nunique() < 2:
        return None
    x = d["tyre_age_this_lap"].astype(float).to_numpy()
    slope, icpt = np.polyfit(x, y.to_numpy(), 1)
    resid = y.to_numpy() - (icpt + slope * x)
    sigma = float(resid.std(ddof=2)) if len(x) > 2 else float(resid.std())
    last_lap, last_age = int(d["lap_number"].max()), float(x[d["lap_number"].to_numpy().argmax()])
    ks = np.arange(0, horizon + 1)
    proj = icpt + slope * (last_age + ks)
    return {"lap": list(d["lap_number"]), "actual": list(y), "proj_lap": list(last_lap + ks), "proj": list(proj),
            "lo": list(proj - 1.645 * sigma), "hi": list(proj + 1.645 * sigma), "slope": float(slope), "intercept": float(icpt),
            "sigma": sigma, "n": int(len(x)), "compound": str(d["tyre_compound"].iloc[-1]), "last_lap": last_lap}


def laps_until_loss(fit: dict, threshold_s: float = 1.0):
    """Laps until the PROJECTED lap time is `threshold_s` slower than the stint's fitted starting pace; None if it never gets there."""
    if not fit or fit["slope"] <= 0:
        return None
    base = fit["proj"][0]
    return int(np.ceil(threshold_s / fit["slope"] - 1e-9)) if base is not None else None


def robust_anomalies(laps: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
    """Per-driver robust z-score (median / MAD) of lap time on green-flag, non-in/out laps. A SCREEN for unusual laps, not a diagnosis."""
    d = laps[~laps["is_in_lap"].fillna(False) & ~laps["is_out_lap"].fillna(False) & laps["lap_time_seconds"].notna()].copy()
    if "track_status" in d:            # SC / VSC / yellow / red laps are slow by definition: not anomalies
        d = d[d["track_status"].fillna("GREEN") == "GREEN"]
    if d.empty:
        return d.assign(z=[], is_anomaly=[])

    def z(s):
        med = s.median(); mad = (s - med).abs().median()
        scale = 1.4826 * mad if mad > 0 else (s.std(ddof=0) or np.nan)
        return (s - med) / scale if scale == scale and scale > 0 else s * 0.0
    d["z"] = d.groupby("driver_number")["lap_time_seconds"].transform(z)
    d["is_anomaly"] = d["z"].abs() > threshold
    return d


def error_metrics(actual, predicted) -> dict:
    a, p = np.asarray(actual, float), np.asarray(predicted, float)
    m = ~(np.isnan(a) | np.isnan(p))
    if not m.any():
        return {"n": 0, "mae": float("nan"), "rmse": float("nan"), "bias": float("nan")}
    e = a[m] - p[m]
    return {"n": int(m.sum()), "mae": float(np.abs(e).mean()), "rmse": float(np.sqrt((e ** 2).mean())), "bias": float(e.mean())}


def pace_model_residuals(laps: pd.DataFrame, model: pd.DataFrame) -> pd.DataFrame:
    """Predicted lap time = fresh_tyre_pace + degradation * tyre_age (the dbt model) vs the actual fuel-corrected lap time."""
    if laps.empty or model.empty:
        return pd.DataFrame(columns=["driver", "lap_number", "actual", "predicted", "residual"])
    m = model[["driver", "tyre_compound", "fresh_tyre_pace_seconds", "degradation_seconds_per_lap"]]
    d = laps[laps["is_representative_lap"] & laps["tyre_age_this_lap"].notna()].merge(m, on=["driver", "tyre_compound"], how="inner")
    d["actual"] = d["fuel_corrected_lap_time_seconds"].where(d["fuel_corrected_lap_time_seconds"].notna(), d["lap_time_seconds"])
    d["predicted"] = d["fresh_tyre_pace_seconds"] + d["degradation_seconds_per_lap"] * d["tyre_age_this_lap"]
    d["residual"] = d["actual"] - d["predicted"]
    return d.dropna(subset=["actual", "predicted"])[["driver", "tyre_compound", "lap_number", "actual", "predicted", "residual"]]


def engineering_alerts(laps: pd.DataFrame, penalties: pd.DataFrame, radio: pd.DataFrame, driver_number: int, upto_lap: int, slow_s: float = 1.5):
    """Rule-based alerts for the focus driver up to `upto_lap`. Heuristics on timing/radio data, not car sensors.
    Lap 1 is skipped (standing start), as are in/out laps and non-green laps."""
    out = []
    d = laps[(laps["driver_number"] == driver_number) & (laps["lap_number"] <= upto_lap)]
    slow = d[(d["lap_number"] > 1) & (d["delta_to_driver_median_seconds"] > slow_s) & ~d["is_in_lap"].fillna(False) & ~d["is_out_lap"].fillna(False) & (d["track_status"].fillna("GREEN") == "GREEN")]
    for r in slow.itertuples():
        out.append(("warn", int(r.lap_number), f"Lap {r.lap_number}: {r.delta_to_driver_median_seconds:+.2f}s slower than the driver's median on a green-flag lap"))
    if not penalties.empty:
        for r in penalties[(penalties["lap_number"] <= upto_lap)].itertuples():
            out.append(("crit", int(r.lap_number or 0), f"Lap {r.lap_number}: penalty ({r.penalty_type}) - {r.reason}"))
    if not radio.empty and "mentions_power_unit" in radio:
        mine = radio[(radio["lap_number"] <= upto_lap) & radio["mentions_power_unit"].fillna(False)]
        if "driver_number" in mine:
            mine = mine[mine["driver_number"] == driver_number]
        for r in mine.itertuples():
            out.append(("warn", int(r.lap_number or 0), f"Lap {r.lap_number}: radio mentions the power unit (keyword match on a transcript)"))
    return sorted(out, key=lambda t: t[1])
