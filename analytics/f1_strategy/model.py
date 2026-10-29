from dataclasses import dataclass, field
from typing import Dict, Tuple
import numpy as np


@dataclass(frozen=True)
class CompoundModel:
    fresh_pace: float      # fuel-corrected lap time (s) on a new tyre
    degradation: float     # seconds lost per lap of tyre age (>= 0)


@dataclass(frozen=True)
class RaceContext:
    total_laps: int
    fuel_effect: float = 0.035            # s per lap of fuel still on board (ASSUMPTION, see dbt var)
    pit_loss: float = 22.0                # green-flag time lost by a stop (s)
    neutralised_pit_discount: float = 0.5  # fraction of pit_loss saved when the stop is made under SC
    sc_hazard_per_lap: float = 0.0        # chance a Safety Car starts on any lap (when none is out)
    sc_duration_laps: float = 3.0         # mean SC length in laps
    sc_lap_time_factor: float = 1.45      # SC lap time relative to the fastest fresh-tyre pace
    noise_sd: float = 0.25                # lap-to-lap random variation (s), independent per lap
    start_penalty: float = 0.0            # fixed time added to every strategy (grid / lap-1 effects)


@dataclass(frozen=True)
class Strategy:
    stints: Tuple[Tuple[str, int], ...]   # ((compound, laps_in_stint), ...) summing to total_laps

    @property
    def stops(self) -> int:
        return len(self.stints) - 1

    @property
    def pit_laps(self):
        out, lap = [], 0
        for _, n in self.stints[:-1]:
            lap += n
            out.append(lap)               # in-lap number (1-based)
        return out

    def label(self) -> str:
        return " > ".join(f"{c}({n})" for c, n in self.stints)


@dataclass
class SimResult:
    strategies: list
    totals: np.ndarray                    # shape (n_strategies, n_sims), seconds
    sc_mask: np.ndarray                   # shape (n_sims, laps), shared by all strategies (paired comparison)

    def summary(self):
        best = self.totals.argmin(axis=0)
        rows = []
        for i, s in enumerate(self.strategies):
            t = self.totals[i]
            rows.append({
                "strategy": s.label(), "stops": s.stops,
                "mean_s": float(t.mean()), "median_s": float(np.median(t)),
                "p10_s": float(np.percentile(t, 10)), "p90_s": float(np.percentile(t, 90)),
                "p_best": float((best == i).mean()),
            })
        return sorted(rows, key=lambda r: r["mean_s"])
