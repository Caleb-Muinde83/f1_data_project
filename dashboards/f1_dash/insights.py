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


def fit_stint_degradation(laps: pd.DataFrame, driver_number: int, upto_lap: int, horizon: int = 10, min_points: int = 4, window: int = 12):
    """ROBUST (Theil-Sen) lap-time-vs-tyre-age fit on the driver's CURRENT stint using only laps <= upto_lap (replay-safe) and at most the last
    `window` clean laps. Theil-Sen takes the median of all pairwise slopes, so restart/warm-up laps after a safety car do not tilt the line the
    way ordinary least squares does (real data: three slow restart laps turned a flat stint into a -0.07 s/lap 'improvement').
    Returns a dict for charts.degradation_projection_fig, or None when there are too few clean laps."""
    d = laps[(laps["driver_number"] == driver_number) & (laps["lap_number"] <= upto_lap)]
    if d.empty:
        return None
    d = d[d["stint_number"] == d.sort_values("lap_number").iloc[-1]["stint_number"]]
    y = d["fuel_corrected_lap_time_seconds"].where(d["fuel_corrected_lap_time_seconds"].notna(), d["lap_time_seconds"])
    ok = d["is_representative_lap"] & y.notna() & d["tyre_age_this_lap"].notna()
    d, y = d[ok].assign(_y=y[ok]).sort_values("lap_number"), None
    d = d.tail(window)
    if len(d) < min_points or d["tyre_age_this_lap"].nunique() < 2:
        return None
    x = d["tyre_age_this_lap"].astype(float).to_numpy()
    yy = d["_y"].to_numpy(float)
    slopes = [(yy[b] - yy[a]) / (x[b] - x[a]) for a in range(len(x)) for b in range(a + 1, len(x)) if x[b] != x[a]]
    slope = float(np.median(slopes))
    icpt = float(np.median(yy - slope * x))
    resid = yy - (icpt + slope * x)
    mad = float(np.median(np.abs(resid - np.median(resid))))
    sigma = 1.4826 * mad if mad > 0 else float(resid.std())
    last_lap, last_age = int(d["lap_number"].max()), float(x[d["lap_number"].to_numpy().argmax()])
    ks = np.arange(0, horizon + 1)
    proj = icpt + slope * (last_age + ks)
    return {"lap": list(d["lap_number"]), "actual": list(yy), "proj_lap": list(last_lap + ks), "proj": list(proj),
            "lo": list(proj - 1.645 * sigma), "hi": list(proj + 1.645 * sigma), "slope": slope, "intercept": icpt,
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
    # A race gets faster as fuel burns off, so raw lap times drift (z-scores from +3 early to -1.5 late on the real data) and the first laps look "unusual".
    # Use the fuel-corrected time where the model provides one (races and sprints); practice and qualifying have none and use the raw time.
    metric = "fuel_corrected_lap_time_seconds"
    if metric in d and d[metric].notna().any():
        d = d[d[metric].notna()]
    else:
        metric = "lap_time_seconds"
    if "track_status" in d:            # SC / VSC / yellow / red laps are slow by definition: not anomalies
        d = d[d["track_status"].fillna("GREEN") == "GREEN"]
    if d.empty:
        return d.assign(z=[], is_anomaly=[])

    def z(s):
        med = s.median(); mad = (s - med).abs().median()
        scale = 1.4826 * mad if mad > 0 else (s.std(ddof=0) or np.nan)
        return (s - med) / scale if scale == scale and scale > 0 else s * 0.0
    d["z"] = d.groupby("driver_number")[metric].transform(z)
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


def usable_pace_fits(df: pd.DataFrame, min_laps: int = 8, max_abs_degradation: float = 1.5, pace_ratio: float = 1.07, max_improving_t: float = 2.0) -> pd.Series:
    """Which rows of fct_compound_pace_model are worth showing. Practice/qualifying fits are often junk (real data: 164 s 'fresh pace' on a 95 s
    circuit, -16 s/lap 'degradation') because push, cool-down and in/out laps are mixed. A fit is usable when it has enough clean laps, a pace within
    `pace_ratio` of the best fit of the SAME session, and a plausible slope. r_squared is deliberately not required: a flat, valid stint has r2 near 0.
    A fit whose pace is significantly IMPROVING (slope below 0 with a t-statistic under -`max_improving_t`, from r_squared and the lap count) is not a degradation
    model either: the car got faster because fuel burned off or the track evolved, so it is dropped. A flat or noisy slope (|t| small) is kept."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    enough = df["clean_laps"] >= min_laps
    group = df["session_name"] if "session_name" in df else pd.Series("all", index=df.index)
    best = df["fresh_tyre_pace_seconds"].where(enough).groupby(group).transform("min")
    ok = enough & (df["fresh_tyre_pace_seconds"] <= pace_ratio * best) & (df["degradation_seconds_per_lap"].abs() <= max_abs_degradation)
    if "r_squared" in df:
        r2 = df["r_squared"].astype(float).clip(lower=0.0, upper=0.999)
        t = np.sign(df["degradation_seconds_per_lap"]) * np.sqrt(r2 * (df["clean_laps"] - 2).clip(lower=1) / (1 - r2))
        ok = ok & ~(t < -max_improving_t)
    return ok


def best_calibration_session(df: pd.DataFrame):
    """The earlier session that gives the simulator the most compounds with usable fits (ties: more usable rows, then the latest)."""
    if df is None or df.empty or "session_key" not in df:
        return None
    ok = df[usable_pace_fits(df)]
    if ok.empty:
        return None
    g = ok.groupby("session_key").agg(compounds=("tyre_compound", "nunique"), rows=("tyre_compound", "size")).reset_index()
    return int(g.sort_values(["compounds", "rows", "session_key"]).iloc[-1]["session_key"])


def field_median_corners(corners: pd.DataFrame, drivers) -> pd.DataFrame:
    """Chosen drivers plus a FIELD MEDIAN row per corner, so 20 bars per corner become a readable comparison."""
    if corners is None or corners.empty or "driver" not in corners:
        return corners
    med = corners.groupby("corner_number", as_index=False).agg(avg_apex_kph=("avg_apex_kph", "median"))
    med["driver"] = "FIELD MEDIAN"
    return pd.concat([corners[corners["driver"].isin(drivers)][["corner_number", "driver", "avg_apex_kph"]], med], ignore_index=True)


def fit_diagnostics(laps: pd.DataFrame, driver_number: int, upto_lap: int, min_points: int = 4) -> str:
    """Why a stint projection / anomaly screen has nothing to show: counts available vs required."""
    d = laps[(laps["driver_number"] == driver_number) & (laps["lap_number"] <= upto_lap)]
    if d.empty:
        return f"This driver has no laps up to lap {upto_lap} (did not start, or has not started yet in the replay)."
    cur = d[d["stint_number"] == d.sort_values("lap_number").iloc[-1]["stint_number"]]
    clean = cur[cur["is_representative_lap"].fillna(False) & cur["tyre_age_this_lap"].notna()]
    return (f"{len(d)} laps so far; the current stint (stint {int(cur['stint_number'].iloc[0])}, {cur['tyre_compound'].iloc[-1]}) has {len(cur)} laps, of which "
            f"{len(clean)} are clean (green-flag, not in/out, within the pace threshold) and {clean['tyre_age_this_lap'].nunique()} distinct tyre ages. "
            f"A fit needs at least {min_points} clean laps with 2+ distinct tyre ages.")
