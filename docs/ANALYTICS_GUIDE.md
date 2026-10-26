# Analytics Guide: what the warehouse can answer, on what assumptions, and what it cannot

Scope of verification: everything below was built and tested against **synthetic OpenF1-shaped data**
(`dbt_f1_transformations/ci/generate_fixtures.py`), never against the live API or your real Parquet files.
Run `dbt build --select staging` against real data first; see "First run on real data".

## The model layers

```
sources (18 raw Parquet globs, union_by_name)
  -> staging  (18 views, 1:1 with endpoints; cast + rename only)
  -> intermediate
        int_track_status_periods   SC / VSC / Red periods rebuilt from race-control messages
        int_lap_context            ONE ROW PER DRIVER PER LAP: stint, tyre age, in/out lap, track status,
                                   weather, position, gaps, representative-lap flag, fuel-corrected time
  -> marts
        dimensions   dim_drivers (latest), dim_driver_session (team per session), dim_sessions, dim_circuits
        pace/tyres   fct_lap_performance, fct_fastest_laps, fct_stints, fct_tire_strategy
        race flow    fct_pit_stops, fct_race_intervals, fct_lap_gaps, fct_overtakes, fct_race_trace,
                     fct_driver_positions
        context      fct_weather_impact, fct_lap_incidents, fct_penalties, fct_team_radio
        results      fct_race_results, fct_points_progression, fct_championship_standings,
                     fct_constructor_standings
        telemetry    fct_telemetry (incremental), fct_telemetry_lap_summary,
                     fct_telemetry_distance_trace (incremental), fct_lap_time_delta_trace
```

## Tunable assumptions (`dbt_project.yml` vars)

| Var | Default | What it affects | Basis |
|---|---|---|---|
| `fuel_effect_seconds_per_lap` | 0.035 | `fuel_corrected_lap_time_seconds`, stint degradation | **Assumption**: a commonly quoted order of magnitude; real values vary by circuit and era. Calibrate per circuit before drawing conclusions. |
| `representative_lap_threshold` | 1.07 | which laps count as comparable | Convention (107%-style outlier cut) relative to the driver's own median clean lap |
| `drs_range_seconds` / `dirty_air_seconds` | 1.0 / 2.0 | DRS-range and dirty-air flags | **Proxy**: real DRS eligibility is judged at detection points the API does not expose |
| `slow_stop_margin_seconds` | 3.0 | `is_slow_stop` | Pit lane time above session median + margin |
| `min_stint_laps_for_degradation` | 5 | degradation regression | Below this the slope is NULL rather than noise |
| `distance_bin_metres` | 10 | resolution of the distance trace | |
| `fastest_lap_bonus_last_year` | 2024 | fastest-lap bonus point | Bonus removed from 2025 |

## Known approximations (read before quoting a number)

* **Safety Car periods** end when "SAFETY CAR IN THIS LAP" is issued, so the lap containing that message is still labelled SC; restart-lap effects are not modelled.
* **`time_lost_seconds`** (pit stops) = in-lap + out-lap - 2 x the driver's median clean lap. It includes any traffic on the out-lap and is not meaningful under SC/VSC (`is_under_neutralisation` marks those).
* **`pit_lane_time_seconds`** is pit entry to exit as reported by OpenF1. It is **not stationary time**; the API does not give that.
* **Position change across a stop** includes on-track passes made on the out-lap.
* **Distance** in the distance trace is integrated from speed (sum of speed x dt). Fine for comparing laps on the same circuit; not a survey-grade track length.
* **Points** use the current Race and Sprint scales (seed `f1_points_system`). Historical scoring systems, shortened-race scales and post-race penalties/disqualifications that arrive after the API's classification are **not modelled**.
* **Penalties** (`fct_penalties`) are parsed from race-control wording; fields that do not parse stay NULL. They are **not** applied to `fct_race_results`.
* **Overtake classification** treats a pass as "probable pit cycle" if either driver pitted within 90 s of it.
* **Weather** is the latest reading at or before lap start (ASOF join); the feed is about one reading per minute.

## What is still NOT built (honest list)

* **Corner / sector geometry.** OpenF1 provides none. Corner-level analysis (apex speed, brake point per corner) needs an external circuit reference table. `fct_telemetry_lap_summary` gives per-lap braking-zone *counts* only.
* **Derived channels**: acceleration, apex speed, trail braking.
* **Undercut / overcut analysis**, pit-window optimisation, race-pace simulation, safety-car probability, car-vs-driver pace separation, qualifying-vs-race pace models, track-evolution modelling.
* **Team radio transcription / text analysis.**
* **BigQuery portability.** The dbt models use DuckDB-specific SQL (`ASOF JOIN`, `read_parquet`, `json_extract_string`, `FILTER` window aggregates). The OpenTofu datasets are only a landing zone.
* **Cloud Composer environment**, source freshness checks, dbt contracts, late-arriving-data watermarks, versioned raw files (files are overwritten by design; a manifest records each run).
* **A semantic layer and finished BI dashboards.**
* **Scheduling** stays manual on purpose (`schedule_interval=None`).

## First run on real data (checklist)

1. `dbt seed && dbt build --select staging` and fix any `Binder Error: Referenced column ... not found`. The new staging models read columns that exist in the current OpenF1 API but that I could not confirm in *your* files: `laps.date_start/i1_speed/i2_speed/st_speed`, `pit.date`, `overtakes.date`, `intervals.date`, `weather.pressure`, `race_control.scope/sector/driver_number`, `session_result.dnf/number_of_laps/gap_to_leader`.
2. `dbt build --select int_lap_context+ --exclude fct_telemetry+` and review the **warn**-level tests.
3. Telemetry: run `fct_telemetry` for one session first and watch memory (two ASOF joins over high-frequency data have only been run on ~28k synthetic rows).
4. Spot-check one race against official timing: fastest lap, pit times, points.

## Tests

* `unique_combination_of_columns` (custom, no `dbt_utils` dependency) enforces the grain of every fact.
* Singular tests in `tests/` cover positive lap times, monotonic lap starts, valid stint ranges, one fastest lap per session, points not inflated, plausible telemetry speed, and (warn) positions in range, pit lane time range, results-vs-laps reconciliation.
* The suite was **mutation-tested**: a duplicated lap, a 999 km/h sample and a duplicated grain were injected into a copy of the fixtures and were caught.
* Python: `airflow/tests` covers sanitiser, 404/422/429 handling, idempotent writes, skip-existing, manifest and GCS-loader logic with a fake API hook.
