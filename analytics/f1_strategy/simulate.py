from typing import Dict, Sequence
import numpy as np
from .model import CompoundModel, RaceContext, Strategy, SimResult


def clean_lap_times(ctx: RaceContext, models: Dict[str, CompoundModel], strategy: Strategy) -> np.ndarray:
    """Deterministic (no noise, no SC) lap times for one strategy: fresh pace + degradation x tyre age + fuel effect."""
    if sum(n for _, n in strategy.stints) != ctx.total_laps:
        raise ValueError(f"stints sum to {sum(n for _, n in strategy.stints)} laps, race has {ctx.total_laps}")
    laps = []
    lap_no = 0
    for compound, n in strategy.stints:
        m = models[compound]
        for age in range(n):
            lap_no += 1
            laps.append(m.fresh_pace + m.degradation * age + ctx.fuel_effect * (ctx.total_laps - lap_no))
    return np.asarray(laps, dtype=float)


def sample_sc_mask(rng: np.random.Generator, n_sims: int, laps: int, hazard: float, mean_duration: float) -> np.ndarray:
    """(n_sims, laps) boolean: True where a Safety Car is out. A new SC can only start when none is out."""
    mask = np.zeros((n_sims, laps), dtype=bool)
    if hazard <= 0:
        return mask
    remaining = np.zeros(n_sims, dtype=int)
    for lap in range(laps):
        start = (remaining == 0) & (rng.random(n_sims) < hazard)
        durations = 1 + rng.poisson(max(mean_duration - 1.0, 0.0), size=n_sims)
        remaining = np.where(start, durations, remaining)
        mask[:, lap] = remaining > 0
        remaining = np.maximum(remaining - 1, 0)
    return mask


def evaluate(ctx: RaceContext, models: Dict[str, CompoundModel], strategies: Sequence[Strategy],
             n_sims: int = 4000, seed: int = 0, sc_mask: np.ndarray = None) -> SimResult:
    """Monte Carlo total race time for every strategy. All strategies share the SAME safety-car draws and lap noise
    (common random numbers), so differences between strategies are not sampling luck. Pass sc_mask to force scenarios."""
    rng = np.random.default_rng(seed)
    if sc_mask is None:
        sc_mask = sample_sc_mask(rng, n_sims, ctx.total_laps, ctx.sc_hazard_per_lap, ctx.sc_duration_laps)
    else:
        n_sims = sc_mask.shape[0]
    noise = rng.normal(0.0, ctx.noise_sd, size=(n_sims, ctx.total_laps)) if ctx.noise_sd > 0 else np.zeros((n_sims, ctx.total_laps))
    fastest_fresh = min(m.fresh_pace for m in models.values())
    sc_time = ctx.sc_lap_time_factor * fastest_fresh
    totals = np.empty((len(strategies), n_sims))
    for i, s in enumerate(strategies):
        base = clean_lap_times(ctx, models, s)
        lap_times = np.where(sc_mask, sc_time, base[None, :] + noise)
        total = lap_times.sum(axis=1) + ctx.start_penalty
        for pit_lap in s.pit_laps:
            discount = np.where(sc_mask[:, pit_lap - 1], ctx.neutralised_pit_discount, 0.0)
            total = total + ctx.pit_loss * (1.0 - discount)
        totals[i] = total
    return SimResult(list(strategies), totals, sc_mask)
