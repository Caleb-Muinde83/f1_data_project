# Change list (against the uploaded archive)

Verified on synthetic data: `make ci-test` = 11 Python tests + 97 dbt models/tests passing. See docs/ANALYTICS_GUIDE.md for
assumptions, limits and the first-run checklist for real data.

## Pre-existing defects fixed
* `fct_race_intervals` referenced `interval_raw`, which `stg_intervals` never produced: **it failed to build** (baseline run: 32/33 models).
* `fct_telemetry` joined car data to location on exact timestamp equality (the two feeds are not aligned), and was a view.
* `schema.yml` documented columns that did not exist (`is_slow_stop`, `is_overall_fastest_lap`, `track_temperature`).
* `export_fct_telemetry` had a hard-coded `F:/...` path and always ran when invoked.
* `Makefile` actually contained a Mermaid diagram (preserved as docs/architecture.mmd).
* Credentials were hard-coded in docker-compose; the base image's Python 3.8 cannot install current dbt-duckdb.
* `requirements.txt` held stray shell commands and an unusable `pandas` pin; `.gitignore` did not cover `.env`.

## New files (39)
* `.github/workflows/ci.yml`
* `.github/workflows/deploy_to_composer.yml`
* `airflow/.env.example`
* `airflow/dags/modules/pipeline_utils.py`
* `airflow/tests/conftest.py`
* `airflow/tests/test_pipeline_utils.py`
* `airflow/tests/test_universal_extractor.py`
* `dbt_f1_transformations/ci/generate_fixtures.py`
* `dbt_f1_transformations/macros/delete_sessions_from_incrementals.sql`
* `dbt_f1_transformations/models/intermediate/int_lap_context.sql`
* `dbt_f1_transformations/models/intermediate/int_track_status_periods.sql`
* `dbt_f1_transformations/models/intermediate/schema.yml`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_gaps.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_lap_time_delta_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_penalties.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_points_progression.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_race_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_stints.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_telemetry_distance_trace.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_telemetry_lap_summary.sql`
* `dbt_f1_transformations/models/marts/analytics/fct_tire_strategy.sql`
* `dbt_f1_transformations/models/marts/core/dim_circuits.sql`
* `dbt_f1_transformations/models/marts/core/dim_driver_session.sql`
* `dbt_f1_transformations/profiles.yml`
* `dbt_f1_transformations/seeds/f1_points_system.csv`
* `dbt_f1_transformations/tests/assert_lap_start_times_increase.sql`
* `dbt_f1_transformations/tests/assert_lap_times_positive.sql`
* `dbt_f1_transformations/tests/assert_one_fastest_lap_per_session.sql`
* `dbt_f1_transformations/tests/assert_pit_lane_time_plausible.sql`
* `dbt_f1_transformations/tests/assert_positions_in_range.sql`
* `dbt_f1_transformations/tests/assert_race_points_not_inflated.sql`
* `dbt_f1_transformations/tests/assert_race_results_have_laps.sql`
* `dbt_f1_transformations/tests/assert_results_have_driver_dimension.sql`
* `dbt_f1_transformations/tests/assert_stint_ranges_valid.sql`
* `dbt_f1_transformations/tests/assert_telemetry_speed_plausible.sql`
* `dbt_f1_transformations/tests/generic/unique_combination_of_columns.sql`
* `docs/ANALYTICS_GUIDE.md`
* `docs/architecture.mmd`
* `docs/example_queries.sql`

## Modified files (32)
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
* `dbt_f1_transformations/models/staging/stg_intervals.sql`
* `dbt_f1_transformations/models/staging/stg_laps.sql`
* `dbt_f1_transformations/models/staging/stg_overtakes.sql`
* `dbt_f1_transformations/models/staging/stg_pit.sql`
* `dbt_f1_transformations/models/staging/stg_race_control.sql`
* `dbt_f1_transformations/models/staging/stg_session_result.sql`
* `dbt_f1_transformations/models/staging/stg_weather.sql`
* `opentofu/main.tf`
* `opentofu/variables.tf`
* `scripts/deploy_to_composer.sh`
