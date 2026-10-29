"""python -m f1_strategy.cli --db warehouse.duckdb --session 9158 [--driver 1] [--top 8] [--sims 2000]"""
import argparse
import duckdb
from .calibrate import load_inputs
from .optimize import enumerate_strategies, rank_strategies


def main(argv=None):
    ap = argparse.ArgumentParser(description="Rank race strategies by simulated total race time")
    ap.add_argument("--db", required=True); ap.add_argument("--session", type=int, required=True)
    ap.add_argument("--driver", type=int); ap.add_argument("--laps", type=int)
    ap.add_argument("--top", type=int, default=8); ap.add_argument("--sims", type=int, default=2000)
    ap.add_argument("--max-stops", type=int, default=2); ap.add_argument("--pit-step", type=int, default=2)
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args(argv)
    con = duckdb.connect(a.db, read_only=True)
    ctx, models, prov = load_inputs(con, a.session, a.driver, a.laps)
    if len(models) < 2:
        raise SystemExit(f"Need >= 2 compounds with a fitted pace model; found {sorted(models)}. Run more laps or lower "
                         "var min_stint_laps_for_degradation.")
    strategies = enumerate_strategies(ctx.total_laps, sorted(models), a.max_stops, a.pit_step)
    print(f"Session {a.session}, {ctx.total_laps} laps, {len(strategies)} candidate strategies, {a.sims} simulations each\n")
    print("Inputs (provenance):")
    for k, v in prov.items(): print(f"  {k}: {v}")
    print(f"\n{'strategy':44s} {'mean':>9s} {'median':>9s} {'p10':>9s} {'p90':>9s} {'P(best)':>8s}")
    for r in rank_strategies(ctx, models, strategies, a.sims, a.seed, a.top):
        print(f"{r['strategy']:44s} {r['mean_s']:9.1f} {r['median_s']:9.1f} {r['p10_s']:9.1f} {r['p90_s']:9.1f} {r['p_best']:8.2%}")


if __name__ == "__main__":
    main()
