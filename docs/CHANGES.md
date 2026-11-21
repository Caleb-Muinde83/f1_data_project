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

* New opt-in variable `telemetry_only_sessions` (list of session keys) limits `fct_telemetry` and, through it, every telemetry mart to those sessions: `dbt build --select fct_telemetry+ --vars '{telemetry_only_sessions: [9158]}'`. Unset, behaviour is unchanged (all sessions with car data).

## Fixes after the first look at REAL data (Shanghai 2026, screenshots)
Data model (each has a new test that fails without the fix, and a fixture that reproduces the real-data failure):
* `fct_telemetry`: a sample is credited to a lap only while that lap is running. Before, the as-of join had no upper bound, so cool-down laps and
  garage time were attached to the previous lap (the final lap of a race held minutes of slow driving: broken overlays, 150 s delta traces,
  93 km/h "top speeds"). **Existing telemetry tables must be rebuilt with `--full-refresh`** (the model is incremental and skips known sessions).
* `fct_race_results`: starting-grid rows are matched directly OR through the session that set the grid (Qualifying for a Race, Sprint Qualifying/Shootout
  for a Sprint). New column `grid_source`; new warn test `assert_race_results_have_grid`.
* `int_lap_context`: cumulative race time falls back to lap-start differences when a lap has no duration, and is NULL for a retiring car's unfinished last
  lap (SUM skipped it and froze his clock: 140 s spikes in the race trace at every retirement). Test: `assert_race_trace_leader_time_increases` (error in CI, warn on other targets: on the first real-data run it flagged 27 laps still to be explained and blocked fct_undercut_overcut).
* `int_track_status_periods`: an SC/VSC with no closing message is capped (VSC 10 min, SC 20 min) instead of running to the session end (a "35-lap VSC").
  New column `is_unclosed`; test `assert_neutralisation_periods_are_bounded`.
Dashboard:
* Unusable practice/qualifying tyre fits (164 s pace, -16 s/lap) are hidden by default (`insights.usable_pace_fits`); the simulator defaults to the earlier
  session with the most usable compounds (usually the Sprint).
* The stint projection uses a robust Theil-Sen fit on the last 12 clean laps, so slow restart laps no longer fake an "improvement".
* Charts default to the focus and comparison drivers (picker) with a FIELD MEDIAN bar; incident tables show one row per event; the sector heatmap hides
  drivers without a push lap and clips colours; pit markers are visible; empty-grid cases explain themselves; a DNF's missing position no longer crashes the
  Data Scientist page. New "Data health" table on the Data coverage page.

## Second real-data round (grid, sprints, race trace)
* **Sprints were never typed 'Sprint'.** OpenF1 reports a Sprint with `session_type = 'Race'` and only `session_name = 'Sprint'`. So sprints were scored on the
  Race points scale (25-18-15...), could not find their grid (the Race branch looks for a Qualifying session that runs AFTER a sprint), and were counted as races in
  `dim_circuits`, the safety-car hazard and events. `stg_sessions` now normalises it. Tests: `assert_sprints_are_typed_sprint`, `assert_sprint_points_use_sprint_scale`.
  Fixtures: session 9003 is now a Sprint exactly as OpenF1 reports it, with a Sprint Qualifying session (9004) that carries its grid.
* **Cumulative race time is built from lap END timestamps** (next lap start, or start + duration for a final lap), relative to the first car to finish lap 1,
  instead of a running SUM of durations. OpenF1 often has no lap-1 start/duration; SUM omitted that lap permanently, so the driver looked ~100 s ahead for the whole
  race (27 laps flagged on real data, all lap 2). Fixture: driver 63 in 9003 has neither.
* Grid evidence from real data: starting_grid rows are keyed to 'Qualifying' (1,484 rows) and 'Sprint Qualifying' (382 rows), never to the race itself.
* Data-health check no longer crashes on a warehouse without any VSC (NA handling).

* **A VSC upgraded to a Safety Car now ends when the SC starts.** Real 2024 Shanghai race-control log: VIRTUAL SAFETY CAR DEPLOYED 07:39:16, SAFETY CAR DEPLOYED 07:43:21,
  SAFETY CAR IN THIS LAP 07:49:36, and no 'VIRTUAL SAFETY CAR ENDING' at all, so the VSC stream never closed (a '35-lap VSC'). `int_track_status_periods` now ends a period
  at the start of a MORE SEVERE one (VSC < SC < RED); the 10-minute cap remains only for periods with neither a closing message nor a superseding one.
  Test: `assert_neutralisation_periods_do_not_overlap` (error in CI, warn elsewhere). Fixture: a VSC -> SC -> 'in this lap' sequence in session 9003.

## Third look at real data (dash1 screenshots, a crash, and a review)
* **Crash fixed:** `session_laps` compared `NA == NA` for a session with no lap rows ("boolean value of NA is ambiguous"), and then the replay slider got min = max = 1. Replay pages now
  say "No lap data for <session>" and stop; a one-lap session shows no slider. Regression tests for both.
* **Drivers without laps (DNS):** the sidebar marks them "(no laps)" and every page banner warns when the focus or comparison driver has no laps, instead of showing blank KPIs.
  (A review of the screenshots claimed the Engineer pages showed another driver's telemetry under the selected driver. They do not: the overlay legend names the driver shown, a caption says
  the selected driver has no telemetry, and the comparison driver is visible in the sidebar. The missing piece was the explicit notice, now added.)
* **Race events:** `data.race_events` puts neutralisations, penalties, pit stops, retirements and the fastest lap on one lap axis (timeline chart on the Analyst pages), and every replay page
  has a 'Jump to event' control that moves the replay lap.
* **Empty states explain themselves** (Data Scientist replay): laps seen, clean laps, distinct tyre ages and the minimum required.
* **Simulator:** when a compound's degradation was clipped to 0 (negative fitted slope) the page says the ranking then reflects only pace and pit timing, not tyre wear.
* `assert_neutralisation_periods_do_not_overlap` now looks at Race/Sprint sessions only (the FIA tests SC/VSC deployments in practice).
* Test hygiene: an autouse fixture restores `f1_dash.data` after tests that monkeypatch it.

## Fourth look at real data (2025 Shanghai, session 9998)
* **Charts are never WebGL.** Plotly Express silently switches to `scattergl` above ~1000 rows (a 20-driver, 56-lap race is 1,120), and WebGL lines can fail to draw: the position
  progression showed no lines and the race trace showed loose dots. `components/plot.show` converts every `scattergl` trace to SVG `scatter` (same data and style). Unit-tested on the
  conversion; the rendering itself still needs a look in your browser.
* **Simulator calibration only offers sessions the simulator can use** (two or more compounds with a fitted pace model; `simulator.viable` applies the same test as `render`). Earlier races
  at the same circuit are now candidates (valid pre-race information) and the race distance defaults to the last race there. The page used to default to a session the simulator then rejected.
* The championship chart on a post-race page stops at the selected race unless 'Show the rest of the season' is ticked.
* Confirmed on real data: the overlap warning in session 9898 was Practice 2 at Baku (FIA SC/VSC tests); the narrowed test passes.

## Fifth look at real data (2025 Shanghai simulator, 2026 penalties)
* **A 'penalty served' notice is not a second penalty.** Real data: OCO showed two 10 s 'causing a collision' penalties (laps 37 and 51); lap 51 is 'FIA STEWARDS: PENALTY SERVED - 10 SECOND
  TIME PENALTY FOR CAR 31 (OCO) - ...' (it is also why his second stop was 47 s). `fct_penalties` drops served notices and attaches their lap to the matching penalty as `served_lap`
  (same session, driver and seconds); the events table says '(served lap N)'. Test: `assert_penalties_not_double_counted` (error in CI, warn elsewhere). Fixture: a penalty and its served notice.
* **The simulator's race distance comes from the last race of the same kind.** It took the most recent earlier session at the circuit, which was the 19-lap Sprint, so every simulated
  strategy was 19 laps long. A Race target now uses the last Race (a Sprint target, the last Sprint). Earlier races at the circuit are offered for calibration, Races first.
* Confirmed on real data: the SVG change fixed the position-progression and race-trace lines; the simulator now offers '2024 Race at this circuit'.

## Sixth look at real data (2026 Engineer pages, 2025 simulator)
See `docs/ISSUES_CHECKLIST.md` for the full list and status of every issue. Changed this round, and only these files: `data.py`, `workspaces/engineer/pre_race.py`,
`workspaces/scientist/pre_race.py`, `charts/predictions.py`, `theme.py` (plus tests and the checklist).
* Engineer pre-race: the baseline defaults to a session that has telemetry, labels the ones that do not, and prints the exact `telemetry_only_sessions` build command.
* Scientist pre-race: race distance = the classified leader's laps (`data.race_distance`), not the largest lap row (which counted laps after the flag: 58 for a 56-lap race).
* Anomaly chart axis clipped at 12; KPI tiles scale with their column.
* Not changed on purpose: corner defaults (the detector counts braking zones, not turns), the penalty test (18 groups awaiting a diagnostic), telemetry models (ALO's data awaiting a diagnostic).

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

## Dashboard review against screenshots (round 7)

Found by reading the dashboard screenshots of the real warehouse; fixes are covered by `dashboards/tests/test_review_fixes.py`, `test_p_best_is_shared...` and `assert_corner_direction_needs_angle`.

* **Corner turn direction was invented.** `dim_session_corners` reported `RIGHT` for every corner whose turn angle was NULL, and the angle was NULL whenever the exact entry or exit distance bin had no sample (car data is about 4 Hz, so at 300 km/h a 10 m bin is often empty). Entry and exit points now use the nearest bin with a position (`corner_geometry_search_bins`), the direction is NULL when the angle is, and `fct_corner_performance` entry/exit speeds and corner time use the bins within 2 of the entry/exit point. Reproduced on synthetic data by deleting the exact entry/exit bins: the old model gave 0 of 9 angles and 9 invented RIGHTs, the new one measures all 9.
* **Corner averages only use complete laps.** `fct_corner_performance` now carries `is_complete_lap` and the dashboard query requires it (the trace model says incomplete laps must not be compared point by point).
* **Strategy simulator `p_best`** gave all the credit of a tie to the first strategy (`argmin`); tied strategies now share it.
* **Strategy chart** drew bars on an axis that did not start at zero; it now draws the mean with P10 to P90 whiskers.
* **Driver colours** are fixed per driver on every chart (they changed from chart to chart and repeated beyond ten drivers).
* **Replay**: a penalty's `served_lap` is hidden until that lap is reached, future events on the event chart are dimmed, and a safety car is drawn as a span.
* **Pace chart** no longer draws the rolling line across missing (safety-car) laps; **telemetry overlay** breaks the line across a data dropout and labels an incomplete lap.
* **Sector heatmap** no longer requires "representative" laps (a best sector is a minimum, so slow laps cannot hurt it); on the real qualifying session the six fastest qualifiers were missing.
* **Labels**: "Championship before this weekend" is now "going into the race" (it includes the Sprint); the safety-car hazard legend shows the event and number of races instead of the circuit key; the fuel-corrected axis is named; the pit-stop chart has a legend; the undercut table can hide stops made under a safety car.
* **Pit stops**: a stop is now neutralised when its in-lap OR out-lap runs under SC/VSC/red (lap-9 stops whose out-lap was the first safety-car lap were counted as green and showed 50 s "time lost"), and a lap-1 stop has no time lost. This feeds `fct_pit_loss_by_circuit` and `avg_green_flag_time_lost_seconds`. Guarded by `assert_green_stops_have_green_laps`.
* **Sector heatmap, second correction**: on the real qualifying session every driver's fastest lap was flagged `is_out_lap`, so excluding in/out laps would have hidden the same drivers; the heatmap now uses every fully timed lap.
* **Still to fix** (found by `diagnostics_real_data.sql`): in qualifying every push lap is flagged `is_out_lap` and the driver's median "clean" lap is 112 to 131 s against 92 s push laps, so `is_representative_lap` is inverted for qualifying and practice. The cause is in how `int_lap_context` combines the API's `is_pit_out_lap` with the pit table; it needs the lap-by-lap query 7 before it is changed.
