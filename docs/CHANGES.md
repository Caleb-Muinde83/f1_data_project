# Round 2: role-based dashboards and review fixes

Implements `modifications.md`. Verified on the synthetic fixtures with `dbt build` (125/125 PASS on DuckDB) and `pytest dashboards/tests`
(54 passed). NOT verified: the BigQuery build (no emulator or credentials in the authoring environment), and the Streamlit layout in a
real browser (pages are tested headlessly with `streamlit.testing`).

## Data model fixes
* `int_lap_context.sql`: removed four standalone `OFFSET 0` clauses (valid in DuckDB, a syntax error in BigQuery).
* `seeds/f1_points_system.csv` is now season-aware (`season, session_type, position, points`; Race 2018-2026, Sprint 3-2-1 in 2021 and 8-1 from 2022).
  `fct_race_results` joins on season. New test `assert_every_season_has_points_scale` fails when a loaded season has no scale instead of scoring 0.
  The BigQuery emulator seed loader was updated for the new column. Check the 2026 Sprint scale if the regulations changed.
* `fct_team_radio` now takes the team from `dim_driver_session` (team at the time of the session), and the semantic layer gains a
  session-aware `driver_sessions` model next to `drivers`.

## Dashboard (dashboards/)
* Navigation is now role x phase: Race Engineer / Race Analyst / Race Data Scientist, each with Pre-race, During race (replay) and Post-race, plus a Data coverage page.
* One shared context (season > Grand Prix > session > focus and comparison driver) in the sidebar. Dark graphite theme (`.streamlit/config.toml`, `f1_dash/theme.py`).
* Every panel carries a provenance tag (measured / derived / estimated / predicted / simulated / historical / manual) and its source.
* "During race" is an honest REPLAY of historical data with a lap slider; it is labelled as not live. There is no live feed in this project.
* Pre-race pages only read data that existed before the race (earlier sessions of the weekend, earlier races); tests guard against leakage.
* New: circuit map, two-lap telemetry overlay, position progression, gap evolution, sector heatmap, timing tower, stint/pit timeline, grid base-rate
  finishing model, replay degradation projection, lap-time anomaly screen, model error metrics and residuals, model registry.
* Things the data cannot provide (setup, tyre temperatures, power-unit state, forecasts) are shown as explicit "not available" panels, not faked.
* `f1_dash/charts.py` became a package (old import paths still work); `pages.py` was replaced by `f1_dash/workspaces/`.

## Fixes after first look in a browser
* Analyst timing tower used the position feed sampled at lap end and could show two cars in one position; it now uses cumulative race time (same source as the position chart).
* Anomaly screen ignores SC/VSC/yellow/red laps (slow by definition). Float noise (1e-13) is rounded out of query results. Empty charts are short. Missing-telemetry traces are explained.

* The sidebar now shows which warehouse file is open and warns loudly when it is the synthetic CI fixture warehouse (an exported `F1_DUCKDB_PATH` had silently pointed `dbt --target dev` and the dashboard at it).
* Circuit map: colour-bar title no longer collides with the legend.

## Fixes after the first real-data run
* `StreamlitDuplicateElementId`: identical charts on one page (for example two 'no telemetry' placeholders) collided. All charts now go through `components/plot.show()`, which gives each a unique key.
* `ResourceInUseException`: the dashboard shared one DuckDB connection across overlapping Streamlit runs; each run now gets its own cursor.
* Regression tests for both, for sessions without telemetry, and for concurrent reads.

* **Real-data defect fixed:** in practice and qualifying OpenF1 reports every garage visit as a pit stop (12,462 + 5,706 rows with median pit-lane times of 340 s and 260 s, against 2,568 race stops at 23.5 s). They flooded `fct_pit_stops`, so circuit-level pit-loss medians (used by the strategy simulator) were meaningless. `fct_pit_stops` now keeps Race and Sprint stops only. New tests: `assert_pit_stops_only_in_races`, `assert_circuit_pit_loss_is_plausible`; the fixtures now include garage visits. `assert_pit_lane_time_plausible` remains a warn-level review of long race stops (red flags, stuck cars).

## Not done (see the Data coverage page)
Serving layer (FastAPI/WebSockets), splitting `int_lap_context`, ML models ML-02/06/07/12, SHAP/feature importance, drift monitoring,
multi-driver field simulation, saved views and cross-role incident workspace.


---

# Round 1: change list (against the uploaded archive)

Run `make ci-test` for the full local suite. See docs/ANALYTICS_GUIDE.md for what each addition does, its assumptions, how it was
verified, and what remains unverified or not built.

## Pre-existing defects found and fixed
* `fct_race_intervals` referenced `interval_raw`, which `stg_intervals` never produced: it failed to build (baseline: 32/33 models).
* `fct_telemetry` joined car data to location on exact timestamps (the feeds are not aligned) and was a view.
* `schema.yml` documented columns that did not exist (`is_slow_stop`, `is_overall_fastest_lap`, `track_temperature`).
* The export macro had a hard-coded `F:/` path and always ran; credentials were hard-coded in docker-compose; the base image's Python 3.8 cannot install current dbt-duckdb.
* `Makefile` contained a Mermaid diagram (kept as docs/architecture.mmd); `requirements.txt` held stray shell commands and an unusable pandas pin.

## Defects found by the new tests while building this version
* Corner detection invented a corner at the lap start (truncated smoothing window); integrated lap distance drifted so corners misaligned between laps (laps are now rescaled to the session's track length).
* `delete_sessions_from_incrementals` never committed its DELETE; the telemetry export comment claimed it created a missing folder (COPY does not).
* BigQuery-only differences found by comparing every table with DuckDB: `LEAST(x, NULL)` returns NULL, no `DEGREES()`, a GROUP BY that DuckDB tolerates, unordered `STRING_AGG`, approximate grouped medians, and engines mishandling windowed MIN/MAX over TIMESTAMP.

## New files (86)
* `.github/workflows/ci.yml`
* `.github/workflows/deploy_to_composer.yml`
* `airflow/.env.example`
* `airflow/dags/modules/pipeline_utils.py`
* `airflow/dags/modules/radio_transcriber.py`
* `airflow/tests/conftest.py`
* `airflow/tests/test_bq_loader.py`
* `airflow/tests/test_pipeline_utils.py`
* `airflow/tests/test_radio_transcriber.py`
* `airflow/tests/test_universal_extractor.py`
* `analytics/f1_strategy/__init__.py`
* `analytics/f1_strategy/calibrate.py`
* `analytics/f1_strategy/cli.py`
* `analytics/f1_strategy/model.py`
* `analytics/f1_strategy/optimize.py`
* `analytics/f1_strategy/simulate.py`
* `analytics/requirements.txt`
* `analytics/tests/conftest.py`
* `analytics/tests/test_strategy.py`
* `dashboards/README.md`
* `dashboards/app.py`
* `dashboards/f1_dash/__init__.py`
* `dashboards/f1_dash/charts.py`
* `dashboards/f1_dash/data.py`
* `dashboards/f1_dash/pages.py`
* `dashboards/requirements.txt`
* `dashboards/tests/conftest.py`
* `dashboards/tests/test_dashboard.py`
* `dbt_f1_transformations/ci/bigquery_emulator/compare_duckdb_bigquery.py`
* `dbt_f1_transformations/ci/bigquery_emulator/prepare.py`
* `dbt_f1_transformations/ci/bigquery_emulator/run_tests.sh`
* `dbt_f1_transformations/ci/bq_emulator_runner.py`
* `dbt_f1_transformations/ci/generate_fixtures.py`
* `dbt_f1_transformations/ci/semantic_check.py`
* `dbt_f1_transformations/macros/delete_sessions_from_incrementals.sql`
* `dbt_f1_transformations/macros/emulator_overrides.sql`
* `dbt_f1_transformations/macros/portable.sql`
* `dbt_f1_transformations/models/intermediate/int_lap_context.sql`
* `dbt_f1_transformations/models/intermediate/int_track_status_periods.sql`
* `dbt_f1_transformations/models/intermediate/schema.yml`
* `dbt_f1_transformations/models/marts/analytics/dim_session_corners.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_compound_pace_model.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_corner_performance.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_gaps.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_time_delta_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_penalties.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_pit_loss_by_circuit.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_points_progression.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_race_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_radio_messages.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_safety_car_events.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_safety_car_hazard.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_stints.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_telemetry_distance_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_telemetry_lap_summary.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_tire_strategy.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_undercut_overcut.sql`
* `dbt_f1_transformations/models/marts/core/dim_circuits.sql`
* `dbt_f1_transformations/models/marts/core/dim_driver_session.sql`
* `dbt_f1_transformations/models/metricflow_time_spine.sql`
* `dbt_f1_transformations/models/semantic/metrics.yml`
* `dbt_f1_transformations/models/semantic/semantic_models.yml`
* `dbt_f1_transformations/models/semantic/time_spine.yml`
* `dbt_f1_transformations/models/staging/stg_radio_transcripts.sql`
* `dbt_f1_transformations/profiles.yml`
* `dbt_f1_transformations/seeds/f1_points_system.csv`
* `dbt_f1_transformations/seeds/schema.yml`
* `dbt_f1_transformations/tests/assert_corner_apex_is_a_minimum.sql`
* `dbt_f1_transformations/tests/assert_lap_start_times_increase.sql`
* `dbt_f1_transformations/tests/assert_lap_times_positive.sql`
* `dbt_f1_transformations/tests/assert_one_fastest_lap_per_session.sql`
* `dbt_f1_transformations/tests/assert_pit_lane_time_plausible.sql`
* `dbt_f1_transformations/tests/assert_positions_in_range.sql`
* `dbt_f1_transformations/tests/assert_race_points_not_inflated.sql`
* `dbt_f1_transformations/tests/assert_race_results_have_laps.sql`
* `dbt_f1_transformations/tests/assert_results_have_driver_dimension.sql`
* `dbt_f1_transformations/tests/assert_safety_car_probability_valid.sql`
* `dbt_f1_transformations/tests/assert_stint_ranges_valid.sql`
* `dbt_f1_transformations/tests/assert_telemetry_speed_plausible.sql`
* `dbt_f1_transformations/tests/assert_undercut_outcome_consistent.sql`
* `dbt_f1_transformations/tests/generic/unique_combination_of_columns.sql`
* `docs/ANALYTICS_GUIDE.md`
* `docs/CHANGES.md`
* `docs/architecture.mmd`
* `docs/example_queries.sql`
* `scripts/load_raw_to_bigquery.py`

## Modified files (43)
* `.gitignore`
* `Makefile`
* `README.md`
* `airflow/dags/dbt_cosmos_dag.py`
* `airflow/dags/extract_openf1_dag.py`
* `airflow/dags/modules/gcs_loader.py`
* `airflow/dags/modules/universal_extractor.py`
* `airflow/docker-compose.yaml`
* `airflow/requirements.txt`
* `dbt_f1_transformations/dbt_project.yml`
* `dbt_f1_transformations/macros/export_fct_telemetry.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_fastest_laps.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_incidents.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_performance.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_overtakes.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_pit_stops.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_race_intervals.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_race_results.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_telemetry.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_weather_impact.sql`
* `dbt_f1_transformations/models/marts/schema.yml`
* `dbt_f1_transformations/models/staging/sources.yml`
* `dbt_f1_transformations/models/staging/stg_car_data.sql`
* `dbt_f1_transformations/models/staging/stg_championship_drivers.sql`
* `dbt_f1_transformations/models/staging/stg_championship_teams.sql`
* `dbt_f1_transformations/models/staging/stg_drivers.sql`
* `dbt_f1_transformations/models/staging/stg_intervals.sql`
* `dbt_f1_transformations/models/staging/stg_laps.sql`
* `dbt_f1_transformations/models/staging/stg_location.sql`
* `dbt_f1_transformations/models/staging/stg_meetings.sql`
* `dbt_f1_transformations/models/staging/stg_overtakes.sql`
* `dbt_f1_transformations/models/staging/stg_pit.sql`
* `dbt_f1_transformations/models/staging/stg_position.sql`
* `dbt_f1_transformations/models/staging/stg_race_control.sql`
* `dbt_f1_transformations/models/staging/stg_session_result.sql`
* `dbt_f1_transformations/models/staging/stg_sessions.sql`
* `dbt_f1_transformations/models/staging/stg_starting_grid.sql`
* `dbt_f1_transformations/models/staging/stg_stints.sql`
* `dbt_f1_transformations/models/staging/stg_team_radio.sql`
* `dbt_f1_transformations/models/staging/stg_weather.sql`
* `opentofu/main.tf`
* `opentofu/variables.tf`
* `scripts/deploy_to_composer.sh`

## Deleted files (3)
* `dbt_f1_transformations/macros/.gitkeep`
* `dbt_f1_transformations/seeds/.gitkeep`
* `dbt_f1_transformations/tests/.gitkeep`
