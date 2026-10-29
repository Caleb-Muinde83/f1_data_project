# Change list (against the uploaded archive)

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
