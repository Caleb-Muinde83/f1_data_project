# Dashboards

    pip install -r dashboards/requirements.txt
    F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb streamlit run dashboards/app.py     # run from the repository root (theme: .streamlit/config.toml)

## Layout

| Role | Pre-race | During race (replay) | Post-race |
|---|---|---|---|
| Race Engineer | baseline telemetry, circuit map, tyres, weather, setup gaps | per-lap telemetry summary, rule-based alerts, radio | telemetry debrief, corners, stints, incidents |
| Race Analyst | grid, standings, circuit history, competitor pace | timing tower, position and gap evolution, pit stops, overtakes | classification, pace, strategy, overtakes, championship |
| Race Data Scientist | finishing-position base rate, hazard, degradation models, simulator | stint projection, anomaly screen, hazard | prediction error, residuals, counterfactual simulator |

Code: `f1_dash/workspaces/<role>/<phase>.py` (pages), `components/` (banner, KPIs, timing tower, simulator, filters), `charts/` (pure figure functions),
`data.py` (read-only queries), `insights.py` (tested helpers), `state.py` (shared context), `provenance.py`, `theme.py`.

## Rules the pages follow

* The warehouse is opened **read-only**. DuckDB allows one writer: if dbt is running you get a clear message, not a hang.
* Historical data is never shown as live. "During race" pages are replays driven by a lap slider and say so.
* Pre-race pages use only information from before the race. The strategy simulator is calibrated from an earlier session and carries a leakage warning,
  because circuit pit loss and safety-car hazard in the marts include every race in the warehouse.
* Each panel is tagged measured / derived / estimated / predicted / simulated / historical / manual. Data the warehouse does not hold is shown as "not available".
* Tests: `F1_DUCKDB_PATH=<warehouse built from the synthetic fixtures> python -m pytest dashboards/tests` (part of `make ci-test`).
* The pages are tested headlessly; **their layout has not been reviewed in a browser**.
