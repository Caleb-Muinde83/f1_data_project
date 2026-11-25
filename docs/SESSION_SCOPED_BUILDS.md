# Session-scoped incremental builds

Each extraction run builds only its own sessions. Implements `Update plan: session-scoped incremental builds`.

## Rules
1. **The run tells dbt what changed.** Airflow passes `session_keys`; dbt never guesses.
2. **Unset scope = as before.** Manual runs, CI and `dbt_scope=full`; the telemetry tables load only sessions they lack.
3. **A scoped session is replaced, never appended** (pre-delete + `delete+insert` on `session_key`).
4. **No `table` model may read scoped data.** `ci/check_scoped_views.py` enforces it.

## Behaviour per option
| Run | dbt |
| --- | --- |
| `latest` / one key / few sessions (<= `dbt_chunk_size`) | one whole-graph build with `session_keys` |
| more sessions than one chunk | non-telemetry models once (`--exclude tag:telemetry_group`), then `--select tag:telemetry_group` per chunk |
| `skip_existing` | scope = only sessions with files actually written |
| only global files written | non-telemetry models only |
| nothing written (and no radio transcription) | dbt skipped |
| `dbt_scope=full` | whole graph, no scope (ignores extraction results) |
| `dbt_full_refresh=true` | `--full-refresh` of everything; needs `allow_large_backfill` |

Ordering note: in the multi-chunk case the non-telemetry models run **first** (the plan listed them last), because the telemetry group reads
`fct_fastest_laps`, `int_lap_context` and `stg_laps`, which must be fresh before corners and delta traces are built.

## Files
- Airflow: `modules/dbt_scope.py` (scope builder, command planner), `modules/dbt_runner.py` (no shell), `universal_extractor.extract_endpoint_result`
  (`written` / `skipped` / `empty`, also recorded in the manifest), DAG params `dbt_scope`, `dbt_full_refresh`, `dbt_chunk_size`.
- dbt: `macros/scope.sql`; scoped views `stg_car_data`, `stg_location`; incremental (`delete+insert`) `fct_telemetry`,
  `fct_telemetry_distance_trace`, `fct_telemetry_lap_summary`, `dim_session_corners`, `fct_corner_performance`, `fct_lap_time_delta_trace`; tag `telemetry_group`.
- Tests: heavy generic tests are scope-aware (`not_null_in_scope`, `unique_combination_of_columns(scope_column=...)`) and the singular telemetry tests use `in_scope()`.
  (Done in the test macros rather than a yml `where`, because schema.yml cannot call project macros.)
- CI: `ci/check_scoped_views.py`, `ci/scoped_equivalence_check.py`, `make scoped-check`.

## Things to know
- `--full-refresh` together with `session_keys` is refused (it would drop every other session).
- `fct_telemetry_lap_summary`, `dim_session_corners`, `fct_corner_performance`, `fct_lap_time_delta_trace` used to be rebuilt fully on every run; unscoped they now
  load only missing sessions. To refresh a session after its laps change, run it with `session_keys`.
- `delete_sessions_from_incrementals` now deletes from every incremental model tagged `telemetry_group`.
- BigQuery keeps `insert_overwrite` per-session partitions; the emulator cannot rerun incrementals, so it is verified by full build only.

## Housekeeping candidates (listed only, nothing deleted)
`__pycache__` from several Python versions in your working tree; `airflow/dags/modules/event_extractor.py` (Chapter 2 first version, only the handbook mentions it)
and the empty stubs `competitor_extractor.py`, `performance_extractor.py`, `race_events_extractor.py`, `results_extractor.py`, `telemetry_extractor.py`;
the stale `AIRFLOW_EXTRA_PIP` comment in `dags/dbt_cosmos_dag.py`; `airflow/dags.zip`, `dbt_f1_transformations.zip`, `persisted_sizes.txt` at the repo root.
