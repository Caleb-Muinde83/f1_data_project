import itertools
from typing import Dict, List, Sequence
from .model import CompoundModel, RaceContext, Strategy
from .simulate import evaluate

DRY = {"SOFT", "MEDIUM", "HARD"}


def enumerate_strategies(total_laps: int, compounds: Sequence[str], max_stops: int = 2, pit_step: int = 1,
                         min_stint: int = 5, require_two_dry_compounds: bool = True) -> List[Strategy]:
    """All strategies with 0..max_stops stops, stint lengths >= min_stint, pit laps on a grid of pit_step laps.
    Regulation modelled: a dry race must use at least two different dry compounds."""
    out = []
    for stops in range(0, max_stops + 1):
        stints_n = stops + 1
        cut_range = range(min_stint, total_laps - min_stint + 1, pit_step)
        for cuts in itertools.combinations(cut_range, stops):
            bounds = [0, *cuts, total_laps]
            lengths = [bounds[i + 1] - bounds[i] for i in range(stints_n)]
            if any(l < min_stint for l in lengths):
                continue
            for seq in itertools.product(compounds, repeat=stints_n):
                if require_two_dry_compounds and len({c for c in seq if c in DRY}) < 2:
                    continue
                out.append(Strategy(tuple(zip(seq, lengths))))
    return out


def rank_strategies(ctx: RaceContext, models: Dict[str, CompoundModel], strategies: Sequence[Strategy],
                    n_sims: int = 2000, seed: int = 0, top: int = 10):
    res = evaluate(ctx, models, strategies, n_sims=n_sims, seed=seed)
    return res.summary()[:top]
