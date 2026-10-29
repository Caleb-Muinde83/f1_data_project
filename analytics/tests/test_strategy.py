import itertools
import duckdb
import numpy as np
import pytest
from f1_strategy import (CompoundModel, RaceContext, Strategy, evaluate, sample_sc_mask, clean_lap_times,
                         enumerate_strategies, rank_strategies, load_inputs)

MODELS = {"SOFT": CompoundModel(89.0, 0.12), "MEDIUM": CompoundModel(89.6, 0.06), "HARD": CompoundModel(90.2, 0.03)}


def det_ctx(**kw):  # no noise, no safety car: fully deterministic
    return RaceContext(total_laps=30, noise_sd=0.0, sc_hazard_per_lap=0.0, **kw)


def test_total_time_matches_hand_calculation():
    ctx = det_ctx(pit_loss=20.0, fuel_effect=0.0)
    s = Strategy((("MEDIUM", 10), ("HARD", 20)))
    expected = sum(89.6 + 0.06 * a for a in range(10)) + sum(90.2 + 0.03 * a for a in range(20)) + 20.0
    got = evaluate(ctx, MODELS, [s], n_sims=3).totals[0]
    assert np.allclose(got, expected)


def test_fuel_effect_is_added_per_lap_of_fuel_remaining():
    ctx = det_ctx(pit_loss=0.0, fuel_effect=0.1)
    s = Strategy((("HARD", 30),))
    base = sum(90.2 + 0.03 * a for a in range(30))
    fuel = 0.1 * sum(30 - l for l in range(1, 31))
    assert np.allclose(evaluate(ctx, MODELS, [s], n_sims=2).totals[0], base + fuel)


def test_stint_lengths_must_sum_to_race_length():
    with pytest.raises(ValueError):
        clean_lap_times(det_ctx(), MODELS, Strategy((("HARD", 10),)))


def test_identical_strategies_give_identical_totals_and_seed_is_deterministic():
    ctx = RaceContext(total_laps=30, sc_hazard_per_lap=0.03)
    s = Strategy((("MEDIUM", 12), ("HARD", 18)))
    a = evaluate(ctx, MODELS, [s, s], n_sims=500, seed=5)
    b = evaluate(ctx, MODELS, [s, s], n_sims=500, seed=5)
    assert np.array_equal(a.totals[0], a.totals[1]) and np.array_equal(a.totals, b.totals)


def test_safety_car_under_pit_lap_reduces_pit_loss_by_the_discount():
    ctx = det_ctx(pit_loss=20.0, neutralised_pit_discount=0.5, fuel_effect=0.0)
    s = Strategy((("MEDIUM", 10), ("HARD", 20)))
    none = np.zeros((1, 30), dtype=bool)
    sc_at_pit = none.copy(); sc_at_pit[0, 9] = True            # pit lap 10 is index 9
    sc_time = ctx.sc_lap_time_factor * 89.0
    green = evaluate(ctx, MODELS, [s], sc_mask=none).totals[0, 0]
    under_sc = evaluate(ctx, MODELS, [s], sc_mask=sc_at_pit).totals[0, 0]
    lap10_green = 89.6 + 0.06 * 9
    assert under_sc == pytest.approx(green - lap10_green + sc_time - 20.0 * 0.5)


def test_sc_sampler_rate_and_no_overlapping_starts():
    rng = np.random.default_rng(1)
    m = sample_sc_mask(rng, 20000, 60, hazard=0.02, mean_duration=3.0)
    p_any = m.any(axis=1).mean()
    assert 0.55 < p_any < 0.75                                  # ~1-(1-0.02*~(1))^60 order of magnitude with overlap suppression
    assert sample_sc_mask(rng, 10, 20, 0.0, 3.0).sum() == 0


def brute_force_best_one_stop(ctx, models, compounds, min_stint=5):
    best = None
    for pit, (c1, c2) in itertools.product(range(min_stint, ctx.total_laps - min_stint + 1), itertools.product(compounds, repeat=2)):
        if c1 == c2:
            continue
        t = clean_lap_times(ctx, models, Strategy(((c1, pit), (c2, ctx.total_laps - pit)))).sum() + ctx.pit_loss
        if best is None or t < best[0]:
            best = (t, c1, c2, pit)
    return best


def test_optimiser_finds_the_brute_force_optimum_without_noise():
    ctx = det_ctx(pit_loss=21.0)
    strategies = enumerate_strategies(ctx.total_laps, sorted(MODELS), max_stops=1, pit_step=1)
    top = rank_strategies(ctx, MODELS, strategies, n_sims=2, top=1)[0]
    t, c1, c2, pit = brute_force_best_one_stop(ctx, MODELS, sorted(MODELS))
    assert top["strategy"] == f"{c1}({pit}) > {c2}({ctx.total_laps - pit})"
    assert top["mean_s"] == pytest.approx(t)


def test_higher_degradation_moves_the_optimal_stop_earlier():
    ctx = det_ctx(pit_loss=20.0)
    def best_pit(deg):
        models = {"MEDIUM": CompoundModel(89.6, deg), "HARD": CompoundModel(90.2, 0.02)}
        strategies = [s for s in enumerate_strategies(30, ["MEDIUM", "HARD"], 1) if s.stints[0][0] == "MEDIUM"]
        return int(rank_strategies(ctx, models, strategies, n_sims=2, top=1)[0]["strategy"].split("(")[1].split(")")[0])
    assert best_pit(0.15) < best_pit(0.08)


def test_two_dry_compound_rule_and_stint_limits():
    ss = enumerate_strategies(40, ["SOFT", "MEDIUM", "HARD"], max_stops=2, pit_step=5, min_stint=8)
    assert ss and all(len({c for c, _ in s.stints}) >= 2 for s in ss)
    assert all(all(n >= 8 for _, n in s.stints) and sum(n for _, n in s.stints) == 40 for s in ss)
    assert not any(s.stops == 0 for s in ss)       # a single-compound no-stop race breaks the rule


def test_more_safety_car_risk_raises_expected_race_time():
    # A SC lap costs ~(1.45-1) x 89 s of extra time; a stop under SC only saves 0.5 x pit loss, so net time must rise.
    s = Strategy((("MEDIUM", 12), ("HARD", 18)))
    lo = evaluate(RaceContext(total_laps=30, sc_hazard_per_lap=0.0, noise_sd=0.0), MODELS, [s], 3000, 1).totals.mean()
    hi = evaluate(RaceContext(total_laps=30, sc_hazard_per_lap=0.08, noise_sd=0.0), MODELS, [s], 3000, 1).totals.mean()
    assert hi > lo + 20


def test_a_stop_is_cheaper_when_a_safety_car_is_out_than_the_same_stop_under_green():
    ctx = det_ctx(pit_loss=20.0, neutralised_pit_discount=0.5, fuel_effect=0.0)
    s = Strategy((("MEDIUM", 10), ("HARD", 20)))
    sc_time = ctx.sc_lap_time_factor * 89.0
    all_sc = np.ones((1, 30), dtype=bool)                       # SC the whole race: only the pit-loss term differs
    t = evaluate(ctx, MODELS, [s], sc_mask=all_sc).totals[0, 0]
    assert t == pytest.approx(30 * sc_time + 20.0 * 0.5)


@pytest.fixture
def warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE dim_sessions AS SELECT 9158 AS session_key, 7 AS circuit_key")
    con.execute("CREATE TABLE int_lap_context AS SELECT 9158 AS session_key, range+1 AS lap_number FROM range(52)")
    con.execute("""CREATE TABLE fct_compound_pace_model AS SELECT * FROM (VALUES
        (9158, 1, 'MEDIUM', 89.5, 0.06, 12), (9158, 44, 'MEDIUM', 89.9, 0.08, 10),
        (9158, 1, 'HARD', 90.1, -0.01, 9), (9158, 44, 'HARD', 90.4, 0.03, 8)) t(session_key, driver_number, tyre_compound,
        fresh_tyre_pace_seconds, degradation_seconds_per_lap, clean_laps)""")
    con.execute("""CREATE TABLE fct_pit_loss_by_circuit AS SELECT 7 AS circuit_key, 23.5 AS median_green_time_lost_seconds,
        22.0 AS median_green_pit_lane_time_seconds, CAST(NULL AS DOUBLE) AS neutralisation_discount_ratio""")
    con.execute("""CREATE TABLE fct_safety_car_hazard AS SELECT 7 AS circuit_key, 'SC' AS track_status, 5 AS laps_remaining,
        0.012 AS shrunk_hazard_per_lap, 2.5 AS avg_duration_laps, TRUE AS low_sample""")
    return con


def test_calibration_uses_driver_values_else_median_and_records_fallbacks(warehouse):
    ctx, models, prov = load_inputs(warehouse, 9158, driver_number=1)
    assert ctx.total_laps == 52 and ctx.pit_loss == 23.5 and ctx.sc_hazard_per_lap == 0.012
    assert models["MEDIUM"].degradation == pytest.approx(0.06)                       # driver 1's own value
    assert models["HARD"].degradation == 0.0                                          # negative slope clipped
    assert "clipped" in prov["HARD.degradation"] and "FALLBACK" in prov["neutralised_pit_discount"]
    assert "LOW SAMPLE" in prov["sc_hazard"]
    _, models_all, _ = load_inputs(warehouse, 9158)
    assert models_all["MEDIUM"].degradation == pytest.approx(0.07)                    # median of 0.06 and 0.08
