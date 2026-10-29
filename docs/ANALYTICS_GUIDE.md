# Analytics Guide

What the warehouse can answer, the assumptions behind each number, how each part was verified, and what is still not done.

## Verification status (read this first)

Everything below was tested against **synthetic OpenF1-shaped data** (`dbt_f1_transformations/ci/generate_fixtures.py`), **never against the live API or your real Parquet files**. "Verified" therefore means "behaves correctly on data that contains the situations I built into it", not "validated against real races".

| Area | How it was verified | Not verified |
|---|---|---|
| dbt models, seed and data tests (DuckDB) | `make ci-test`: 123 dbt nodes (models, seed, tests) pass; tests were mutation-tested (injected duplicate lap, 999 km/h sample) | real data; telemetry at full-season scale |
| BigQuery port | `make bq-emulator-test`: 121 nodes (models and tests; the seed is pre-loaded) pass **and all 56 tables are identical to DuckDB** (BigQuery emulator, real `dbt-bigquery` adapter) | real BigQuery; incremental **re-runs** (emulator cannot run dbt's MERGE); semantic layer and time spine on BigQuery; cost/performance |
| Strategy simulator | 12 unit tests incl. hand-calculated totals and a brute-force optimum | accuracy against real races (no validation) |
| Semantic layer (MetricFlow) | `mf validate-configs` (0 errors) + 7 metrics cross-checked against hand-written SQL | BI tools consuming it |
| Dashboards (Streamlit) | 12 headless tests: every page renders, charts built, error paths | **never looked at in a browser**: layout and styling are unreviewed |
| Radio transcription | 3 unit tests with a fake model | **real speech-to-text** (faster-whisper was never run; model download and audio fetch are blocked here) |
| Airflow DAG | parsed with real Airflow 2.10.5 (27 tasks) | running in Docker; your pinned 2.8.1; dbt-inside-Airflow |
| OpenTofu, Composer deploy, GCS loader against real GCS | unit tests only (fake hook) | `tofu validate/plan` (no binary here), any real GCP resource |

## Model layers

```
sources (raw Parquet globs, union_by_name)  ->  staging (views)  ->  intermediate  ->  marts
  intermediate:  int_track_status_periods, int_lap_context (one row per driver per lap: the analytical spine)
  dimensions:    dim_drivers, dim_driver_session, dim_sessions, dim_circuits, dim_session_corners (DERIVED)
  pace/tyres:    fct_lap_performance, fct_fastest_laps, fct_stints, fct_tire_strategy, fct_compound_pace_model
  race flow:     fct_pit_stops, fct_pit_loss_by_circuit, fct_undercut_overcut, fct_race_intervals, fct_lap_gaps,
                 fct_overtakes, fct_race_trace, fct_driver_positions
  risk/context:  fct_safety_car_events, fct_safety_car_hazard, fct_weather_impact, fct_lap_incidents, fct_penalties
  results:       fct_race_results, fct_points_progression, fct_championship_standings, fct_constructor_standings
  telemetry:     fct_telemetry*, fct_telemetry_lap_summary, fct_telemetry_distance_trace*, fct_lap_time_delta_trace,
                 fct_corner_performance            (* incremental by session)
  radio:         fct_radio_messages (empty unless var enable_radio_transcripts: true)
```

## Newly added capabilities

### Corner geometry and apex speeds (derived, not official)
OpenF1 publishes no corner data, so corners are **detected** from telemetry (`dim_session_corners`): per session, take the fastest lap that has telemetry, smooth its speed along distance, and call an apex a local speed minimum at least `corner_min_speed_drop_kph` below the surrounding peak. `fct_corner_performance` then gives entry/apex/exit speed, brake-to-apex distance and throttle pick-up per driver per lap per corner.
* `corner_number` = "the Nth detected slowing zone", **not** the official turn number; it can differ between sessions.
* Distance is integrated from speed and each complete lap is rescaled to the session's typical track length; without this, corners misalign by tens of metres (found while testing).
* `turn_direction` uses the raw x/y convention and may be mirrored. Apexes within 150 m of the lap start/end are not detected.

### Undercut / overcut (`fct_undercut_overcut`)
For every pair of related stops (within `undercut_window_laps`) where the attacker ran directly behind the defender: who stopped first, the gap before and after both stops (from cumulative lap time), and whether the attacker ended ahead. Position swaps that reverse within a single lap are invisible; stops under SC/VSC are kept but flagged.

### Safety-car probability and race simulation
* `fct_safety_car_hazard`: P(at least one SC/VSC start in the next N laps) per circuit, from a constant per-lap hazard shrunk toward the all-circuit rate (empirical Bayes). **OpenF1 history is short, so per-circuit estimates are weak**; check `low_sample`. Red flags, lap-1 incidents and weather are not modelled.
* `analytics/f1_strategy`: Monte Carlo simulator and strategy optimiser fed by `fct_compound_pace_model`, `fct_pit_loss_by_circuit` and the hazard table. All strategies share the same random draws, so differences are not sampling luck. `make strategy SESSION=9158`. **It ignores traffic, tyre cliffs, track evolution and rival reactions and has not been validated against real races: use it to compare strategies, not to predict a result.** Every input that is a fallback rather than measured is printed.

### Radio transcription
`airflow/dags/modules/radio_transcriber.py` (opt-in DAG param `transcribe_radio`, self-hosted faster-whisper) writes `radio_transcripts_<session>_extracted.parquet`; `fct_radio_messages` joins it to lap/tyre/track status and adds **keyword** tags. Tags are substring heuristics on speech-to-text output, not NLP; transcripts of noisy radio can be wrong.

### BigQuery
The models call portable macros (`macros/portable.sql`) instead of DuckDB-only syntax. Three things that bit during porting and are worth knowing:
* `ASOF JOIN` does not exist in BigQuery: it is emulated in linear time (running MAX of the right-side timestamp over a merged timeline, then an equality join). Right-side rows must be unique per (keys, timestamp); duplicates are collapsed.
* `LEAST(60, NULL)` is 60 in DuckDB but NULL in BigQuery; `DEGREES()`, `FILTER (WHERE ...)`, `REGR_SLOPE`, grouped exact `MEDIAN` and `LIST()` also do not exist there (replaced).
* Telemetry tables are partitioned **one session per partition** so `insert_overwrite` can only replace the session being loaded. `bq_session_key_end` (default 13000) must be raised before the season counter passes it.
Load raw data with `scripts/load_raw_to_bigquery.py`, then `dbt build --target bigquery`. Cross-file type drift (a number in one file, `+1 LAP` in another) is reconciled by the loader because BigQuery columns have a single type.

### Semantic layer and dashboards
* `models/semantic/`: MetricFlow semantic models (6) and 22 metrics (pace, pit stops, results, overtaking). `make semantic-validate`; e.g. `mf query --metrics avg_representative_lap_time_seconds --group-by driver__full_name`.
* `dashboards/`: Streamlit app with Race overview, Pace and tyres, Strategy (stints, pit stops, undercut/overcut, simulator), Telemetry (delta trace, corners), Safety car risk, Team radio. `make dashboard`. It opens the warehouse read-only and says so clearly if dbt holds the lock.

## Tunable assumptions (`dbt_project.yml` vars)

| Var | Default | Used for | Basis |
|---|---|---|---|
| `fuel_effect_seconds_per_lap` | 0.035 | fuel-corrected lap time, degradation, simulator | **Assumption**: a commonly quoted order of magnitude; calibrate per circuit |
| `representative_lap_threshold` | 1.07 | which laps are comparable | convention, relative to the driver's median clean lap |
| `drs_range_seconds` / `dirty_air_seconds` | 1.0 / 2.0 | DRS-range / dirty-air flags | **proxy**: real DRS eligibility is judged at detection points the API does not expose |
| `slow_stop_margin_seconds` | 3.0 | `is_slow_stop` | session median + margin |
| `min_stint_laps_for_degradation` | 5 | degradation / compound models | below this the slope is NULL |
| `corner_*` | see file | corner detection | tuned on synthetic data only: expect to retune on real laps |
| `undercut_window_laps`, `undercut_max_gap_seconds` | 8, 8.0 | undercut pairing | judgement |
| `sc_prior_laps` | 300 | safety-car shrinkage strength | judgement |
| `fastest_lap_bonus_last_year` | 2024 | fastest-lap point | bonus removed from 2025 |

## Known approximations
* SC periods end when "in this lap" is issued; restart effects are not modelled.
* `time_lost_seconds` (pit) = in-lap + out-lap - 2 x median clean lap; includes out-lap traffic; not meaningful under SC/VSC. `pit_lane_time_seconds` is entry-to-exit, **not** stationary time.
* Points use the current Race/Sprint scales only; post-race penalties are not applied. `fct_penalties` parses race-control wording and leaves unparsed fields NULL.
* "Probable pit-cycle pass" = either driver pitted within 90 s of the pass.

## Still not done
* Real BigQuery verification, incremental re-runs on BigQuery, semantic layer on BigQuery.
* Official corner numbers/names (needs an external circuit reference table), acceleration/trail-braking channels.
* Validation of the simulator, corner detection and safety-car model against real races.
* A Cloud Composer environment, Cosmos DAG run, source freshness checks, dbt contracts, late-arriving-data watermarks.
* Radio: real transcription run, any language-model analysis.
* Visual review of the dashboards.

## First run on real data (checklist)
1. `dbt seed && dbt build --select staging` and fix any `Binder Error: Referenced column ... not found`. Columns the staging models read that I could not confirm in *your* files: `laps.date_start/i1_speed/i2_speed/st_speed`, `pit.date`, `overtakes.date`, `intervals.date`, `weather.pressure`, `race_control.scope/sector/driver_number`, `session_result.dnf/number_of_laps/gap_to_leader`.
2. `dbt build --select int_lap_context+ --exclude fct_telemetry+` and review warn-level tests.
3. Telemetry: run one session first and watch memory (the ASOF joins have only run on ~28k synthetic rows).
4. Spot-check one race against official timing: fastest lap, pit times, points, corner count.
