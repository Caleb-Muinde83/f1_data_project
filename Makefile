.PHONY: fixtures unit ci-test dbt-build dbt-docs export airflow-up airflow-down
SHELL := /bin/bash
DBT := cd dbt_f1_transformations && dbt

fixtures:            ## synthetic OpenF1-shaped Parquet in .ci_raw/ (no API calls)
	python dbt_f1_transformations/ci/generate_fixtures.py .ci_raw

unit:                ## Python unit tests (extractor, manifest, GCS loader)
	python -m pytest -q airflow/tests

ci-test: fixtures unit   ## everything CI runs: unit tests + dbt build on synthetic data
	cd dbt_f1_transformations && F1_RAW_DATA_PATH=$(CURDIR)/.ci_raw F1_DUCKDB_PATH=$(CURDIR)/.ci_raw/ci.duckdb dbt build --profiles-dir . --target ci

dbt-build:           ## build against YOUR real data (target dev; see profiles.yml)
	$(DBT) build --profiles-dir . --target dev

dbt-docs:
	$(DBT) docs generate --profiles-dir . --target dev && $(DBT) docs serve --profiles-dir .

export:              ## partitioned telemetry export of all sessions (long); the parent folder must exist
	mkdir -p output && $(DBT) run-operation export_fct_telemetry --profiles-dir . --vars '{export_data: true}'

airflow-up:          ## needs airflow/.env (copy airflow/.env.example)
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down
