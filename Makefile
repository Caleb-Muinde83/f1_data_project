.PHONY: fixtures unit ci-test dbt-build dbt-docs export airflow-up airflow-down dashboard strategy semantic-validate bq-emulator-test
SHELL := /bin/bash
DBT := cd dbt_f1_transformations && dbt
CI_VARS := {enable_radio_transcripts: true, min_stint_laps_for_degradation: 3}

fixtures:            ## synthetic OpenF1-shaped Parquet in .ci_raw/ (no API calls)
	python dbt_f1_transformations/ci/generate_fixtures.py .ci_raw/raw

unit:                ## Python unit tests: extractor/loader/GCS/radio (airflow), strategy simulator (analytics)
	python -m pytest -q airflow/tests
	python -m pytest -q analytics/tests

ci-test: fixtures unit   ## everything CI runs: unit tests, full dbt build + tests on synthetic data, dashboard tests
	cd dbt_f1_transformations && F1_RAW_DATA_PATH=$(CURDIR)/.ci_raw/raw F1_DUCKDB_PATH=$(CURDIR)/.ci_raw/ci.duckdb dbt seed --profiles-dir . --target ci
	cd dbt_f1_transformations && F1_RAW_DATA_PATH=$(CURDIR)/.ci_raw/raw F1_DUCKDB_PATH=$(CURDIR)/.ci_raw/ci.duckdb dbt build --profiles-dir . --target ci --vars '$(CI_VARS)'
	F1_DUCKDB_PATH=$(CURDIR)/.ci_raw/ci.duckdb python -m pytest -q dashboards/tests

dbt-build:           ## build against YOUR real data (target dev; see profiles.yml)
	$(DBT) build --profiles-dir . --target dev

dbt-docs:
	$(DBT) docs generate --profiles-dir . --target dev && $(DBT) docs serve --profiles-dir .

export:              ## partitioned telemetry export of all sessions (long); the parent folder must exist
	mkdir -p output && $(DBT) run-operation export_fct_telemetry --profiles-dir . --vars '{export_data: true}'

dashboard:           ## Streamlit dashboards over your warehouse (set F1_DUCKDB_PATH)
	streamlit run dashboards/app.py

strategy:            ## rank race strategies: make strategy SESSION=9158 [DRIVER=1]
	PYTHONPATH=analytics python -m f1_strategy.cli --db $${F1_DUCKDB_PATH:-f1_local_warehouse.duckdb} --session $(SESSION) $(if $(DRIVER),--driver $(DRIVER),)

semantic-validate:   ## validate the MetricFlow semantic layer (needs: pip install dbt-metricflow)
	cd dbt_f1_transformations && dbt build --select metricflow_time_spine --profiles-dir . --target dev && mf validate-configs && python ci/semantic_check.py

bq-emulator-test:    ## BigQuery equivalence test (needs BQ_EMULATOR_BIN and BQ_PYTHON, see the script header)
	bash dbt_f1_transformations/ci/bigquery_emulator/run_tests.sh

airflow-up:          ## needs airflow/.env (copy airflow/.env.example)
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down
