#!/usr/bin/env bash
# Prove that the whole dbt project builds and passes its tests on BigQuery AND yields the same data as DuckDB.
# Uses goccy/bigquery-emulator, so it needs no Google credentials. A pass is strong evidence, not proof, of identical
# behaviour on real BigQuery (the emulator implements a subset of GoogleSQL).
#
#   BQ_EMULATOR_BIN=/path/to/bigquery-emulator \
#   BQ_PYTHON=/path/to/venv/bin/python      (a Python with dbt-bigquery, google-cloud-bigquery, duckdb, pyarrow) \
#   DBT_DUCKDB=dbt                          (a dbt with dbt-duckdb; default: `dbt` on PATH) \
#   dbt_f1_transformations/ci/bigquery_emulator/run_tests.sh
#
# Known emulator limits (handled here, not in the project): no CSV/Parquet loads (JSON loads + a seed helper are used),
# MERGE only accepts a bare table source (so incremental RE-RUNS cannot be tested), and it is slow on large joins
# (telemetry fixtures are cut to one lap per driver).
set -euo pipefail
: "${BQ_EMULATOR_BIN:?set BQ_EMULATOR_BIN to the bigquery-emulator binary}"
BQ_PYTHON=${BQ_PYTHON:-python}; DBT_DUCKDB=${DBT_DUCKDB:-dbt}
HERE=$(cd "$(dirname "$0")" && pwd); DBT_DIR=$(cd "$HERE/../.." && pwd); ROOT=$(cd "$DBT_DIR/.." && pwd)
WORK=$(mktemp -d); trap 'kill ${EMU_PID:-0} 2>/dev/null || true; rm -rf "$WORK"' EXIT
PORT=${BQ_PORT:-9050}; PROJECT=f1test; ENDPOINT=http://localhost:$PORT
VARS='{enable_radio_transcripts: true, min_stint_laps_for_degradation: 3}'

echo "== 1. synthetic data (1 lap of telemetry per driver)"
FIXTURE_TELEMETRY_LAPS=1 "$BQ_PYTHON" "$DBT_DIR/ci/generate_fixtures.py" "$WORK/raw" >/dev/null

echo "== 2. DuckDB reference build"
cp -r "$DBT_DIR" "$WORK/proj" && rm -rf "$WORK/proj"/{target,logs,dbt_packages} "$WORK/proj"/{packages.yml,package-lock.yml}
( cd "$WORK/proj" && F1_RAW_DATA_PATH="$WORK/raw" F1_DUCKDB_PATH="$WORK/ref.duckdb" "$DBT_DUCKDB" seed --profiles-dir . --target ci >/dev/null \
  && F1_RAW_DATA_PATH="$WORK/raw" F1_DUCKDB_PATH="$WORK/ref.duckdb" "$DBT_DUCKDB" build --profiles-dir . --target ci --vars "$VARS" | tail -3 )

echo "== 3. start BigQuery emulator, load raw data"
"$BQ_EMULATOR_BIN" --project=$PROJECT --dataset=f1_raw --port=$PORT --log-level=error >"$WORK/emulator.log" 2>&1 &
EMU_PID=$!
for _ in $(seq 1 40); do curl -s -m 2 -o /dev/null "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets" && break; sleep 0.5; done
"$BQ_PYTHON" "$ROOT/scripts/load_raw_to_bigquery.py" --raw-dir "$WORK/raw" --project $PROJECT --emulator "$ENDPOINT" --format json | tail -3
"$BQ_PYTHON" "$HERE/prepare.py" $PROJECT "$ENDPOINT" f1_analytics "$DBT_DIR/seeds/f1_points_system.csv"

echo "== 4. dbt build (models + tests) on BigQuery"
( cd "$WORK/proj" && F1_BQ_PROJECT=$PROJECT F1_RAW_SCHEMA=f1_raw F1_BQ_DATASET=f1_analytics F1_BQ_API_ENDPOINT=$ENDPOINT \
  "$BQ_PYTHON" ci/bq_emulator_runner.py build --target bigquery_emulator --profiles-dir . \
  --exclude metricflow_time_spine f1_points_system --vars "$VARS" | sed 's/\x1b\[[0-9;]*m//g' | grep -E "Done\.|ERROR|FAIL" )

echo "== 5. compare every table with the DuckDB reference"
"$BQ_PYTHON" "$HERE/compare_duckdb_bigquery.py" "$WORK/ref.duckdb" $PROJECT f1_analytics "$ENDPOINT"
