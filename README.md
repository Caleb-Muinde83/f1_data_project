# OpenF1 Data Engineering Project

An end-to-end Formula 1 data platform. It pulls race, timing and telemetry data from the [OpenF1 API](https://openf1.org/), stores it as Parquet, builds an analytical warehouse with dbt, and serves it through Streamlit dashboards, a strategy simulator and a MetricFlow semantic layer.

The full step-by-step teaching text for this repository is in [`F1_Data_Engineering_Handbook.md`](F1_Data_Engineering_Handbook.md).

## How the data flows

| Stage | What happens | Where |
|---|---|---|
| 1. Extract | An Airflow DAG calls the OpenF1 API through a custom hook, handling pagination and 422/429 retries | `airflow/dags/extract_openf1_dag.py`, `airflow/dags/modules/universal_extractor.py` |
| 2. Load / stage | Each endpoint and session is written to a deterministic Parquet path (re-runs are idempotent), and a JSON manifest records every file's row count and size | `airflow/data/raw/`, `airflow/dags/modules/pipeline_utils.py` |
| 3. Transform | `dbt build` turns the Parquet files into staging views, two intermediate models and 34 marts in DuckDB (BigQuery is supported through portable SQL) | `dbt_f1_transformations/` |
| 4. Present | A Streamlit app reads the warehouse read-only; the strategy simulator and the semantic layer sit on the same marts | `dashboards/`, `analytics/`, `dbt_f1_transformations/models/semantic/` |

Optional tasks in the same DAG upload raw files to Google Cloud Storage, transcribe team-radio audio into Parquet transcripts, and run dbt inside Airflow.

## Tech stack

* **Source:** OpenF1 API, 18 endpoints (telemetry, location, laps, stints, pit stops, race control, weather, results and more)
* **Orchestration:** Apache Airflow 2.8.1 on Python 3.11, run locally with Docker Compose
* **Warehouse:** DuckDB (local); BigQuery through the `bigquery` dbt target
* **Transformation:** dbt with the `dbt-duckdb` adapter
* **Presentation:** Streamlit and Plotly, Monte Carlo strategy simulator, MetricFlow metrics
* **Infrastructure as code:** OpenTofu (GCS bucket and BigQuery datasets)
* **CI/CD:** GitHub Actions

## Prerequisites

| Tool | Needed for |
|---|---|
| Git | Version control |
| Python 3.11 (3.9 or newer works locally) | dbt, dashboards, tests |
| Docker Desktop (with Compose) | Running Airflow locally |
| Google Cloud CLI and OpenTofu | Optional: only for the GCS/BigQuery path |

Install the Python packages you need into a virtual environment:

```bash
python -m venv venv
source venv/bin/activate            # Windows (Git Bash): source venv/Scripts/activate
pip install dbt-duckdb pyarrow pandas numpy pytest streamlit plotly duckdb
pip install -r dashboards/requirements.txt
```

## Quick start

### 1. Check everything works (no API, no Docker)

```bash
make ci-test
```

This generates a small synthetic OpenF1-shaped dataset, runs the Python unit tests, runs `dbt seed` and `dbt build` (all models and tests) on it, and runs the dashboard tests.

If `dbt deps` cannot reach the dbt package hub from your network, the project still builds without packages: no model uses them, so you can move `dbt_f1_transformations/packages.yml` and `package-lock.yml` aside.

### 2. Run the pipeline on real data

```bash
cp airflow/.env.example airflow/.env      # then fill in the passwords and keys
make airflow-up                            # Airflow UI at http://localhost:8080
```

Generate the two keys with:

```bash
python -c "import secrets; print(secrets.token_hex(32))"                              # AIRFLOW_WEBSERVER_SECRET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # AIRFLOW_FERNET_KEY
```

Trigger `extract_openf1_dag`, then build the warehouse:

```bash
make dbt-build                             # dbt build, target dev
```

Stop Airflow with `make airflow-down`. Your Parquet data and warehouse stay on disk.

### 3. Explore

```bash
F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb make dashboard
make strategy SESSION=9158                 # rank race strategies for a session (optional DRIVER=1)
make semantic-validate                     # needs: pip install dbt-metricflow
```

Run `make dashboard` from the repository root so Streamlit picks up `.streamlit/config.toml`.

## Make targets

| Target | What it does |
|---|---|
| `make fixtures` | Generate the synthetic Parquet dataset in `.ci_raw/` |
| `make unit` | Python unit tests (extractor, loaders, strategy simulator) |
| `make ci-test` | Everything CI runs: unit tests, dbt seed and build on synthetic data, dashboard tests |
| `make dbt-build` | `dbt build` against your real data (target `dev`) |
| `make dbt-docs` | Generate and serve the dbt documentation site |
| `make export` | Partitioned Parquet export of all telemetry (long) |
| `make dashboard` | Streamlit dashboards over your warehouse |
| `make strategy` | Monte Carlo strategy ranking |
| `make semantic-validate` | Validate the MetricFlow semantic layer |
| `make bq-emulator-test` | Build on the BigQuery emulator and compare table by table with DuckDB |
| `make airflow-up` / `make airflow-down` | Start or stop local Airflow |

## Configuration

Paths and targets are set by environment variables, with defaults for a local Windows setup:

| Variable | Meaning |
|---|---|
| `F1_RAW_DATA_PATH` | Folder holding the raw Parquet files |
| `F1_DUCKDB_PATH` | DuckDB warehouse file |
| `F1_EXPORT_PATH` | Output folder for the telemetry export |
| `F1_BQ_PROJECT`, `F1_BQ_DATASET`, `F1_BQ_LOCATION` | BigQuery target settings |
| `ALERT_WEBHOOK_URL` | Optional Slack webhook for task-failure alerts |
| `GCS_RAW_BUCKET` | Optional bucket for the raw-file upload task |

dbt targets are defined in `dbt_f1_transformations/profiles.yml`: `dev` (your machine), `container` (inside Airflow), `ci` (synthetic data), `bigquery`, and `bigquery_emulator`. For example:

```bash
cd dbt_f1_transformations
dbt build --profiles-dir . --target dev
```

Telemetry is large. To build it for chosen sessions only (those sessions are replaced, never appended):

```bash
dbt build --vars '{session_keys: [9158]}'
```

Airflow does this for you: with `run_dbt_build` on, each run passes the sessions it actually wrote. See `docs/SESSION_SCOPED_BUILDS.md`.

## Optional: Google Cloud setup

Only needed for the GCS and BigQuery path.

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable bigquery.googleapis.com storage.googleapis.com composer.googleapis.com \
    iam.googleapis.com cloudresourcemanager.googleapis.com
```

Set your project ID in `opentofu/terraform.tfvars`, then run `tofu init` and `tofu plan` inside `opentofu/`. To load raw files into BigQuery use `scripts/load_raw_to_bigquery.py`.

## Git workflow and CI

Work on feature branches and open a pull request; do not commit directly to `main`.

* `.github/workflows/ci.yml` runs on every pull request: Python unit tests, a synthetic-data dbt build with all tests, and the dashboard tests. A BigQuery-emulator comparison job also runs but is advisory.
* `.github/workflows/deploy_to_composer.yml` is **manual only** (`workflow_dispatch`, dry run by default). It syncs DAGs and plugins to a Cloud Composer bucket through `scripts/deploy_to_composer.sh`.

## Repository structure

```text
f1_data_project/
├── .github/workflows/        ci.yml, deploy_to_composer.yml
├── Makefile
├── F1_Data_Engineering_Handbook.md
├── airflow/
│   ├── Dockerfile            Airflow image with dbt-duckdb and dependencies baked in
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── .env.example
│   ├── dags/                 extract_openf1_dag.py, dbt_cosmos_dag.py, modules/
│   ├── plugins/              hooks/openf1_hook.py, slack_alerts.py
│   └── tests/                extractor, manifest, GCS, radio and BigQuery loader tests
├── dbt_f1_transformations/
│   ├── dbt_project.yml, profiles.yml, packages.yml
│   ├── seeds/                f1_points_system.csv (season-aware)
│   ├── macros/               portable.sql and export/cleanup macros
│   ├── models/
│   │   ├── staging/          19 staging views
│   │   ├── intermediate/     int_track_status_periods, int_lap_context
│   │   ├── marts/core/       dim_drivers, dim_driver_session, dim_sessions, dim_circuits
│   │   ├── marts/analytics/  fct_* marts and dim_session_corners
│   │   └── semantic/         MetricFlow semantic models and metrics
│   ├── tests/                24 singular tests plus a generic grain test
│   └── ci/                   synthetic data generator, semantic check, BigQuery emulator runner
├── analytics/f1_strategy/    Monte Carlo strategy simulator and its tests
├── dashboards/               Streamlit app: data.py, charts/, components/, workspaces/
├── opentofu/                 GCS bucket and BigQuery datasets
├── scripts/                  deploy_to_composer.sh, load_raw_to_bigquery.py
└── docs/                     ANALYTICS_GUIDE.md, CHANGES.md, ISSUES_CHECKLIST.md,
                              example_queries.sql, architecture.mmd
```

## Dashboards

Navigation is organised by role (Race Engineer, Race Analyst, Race Data Scientist) and race phase (pre-race, during-race replay, post-race), plus a data-coverage page. The warehouse is opened read-only, replays are never presented as live data, and each panel is tagged as measured, derived, estimated, predicted, simulated or historical. See `dashboards/README.md`.

## Verification status

`make ci-test` was run on synthetic data and passes: Python unit tests (18 airflow, 12 strategy), 135 dbt nodes (56 models, 1 seed, 78 tests) on DuckDB 1.5.6 and 2.0.0, and 104 dashboard test cases.

Not verified: the Docker containers and dbt inside Airflow, real BigQuery, GCS and Composer, OpenTofu changes, real speech-to-text, and the full pipeline on real data. `docs/ANALYTICS_GUIDE.md` has the full table, and `docs/CHANGES.md` and `docs/ISSUES_CHECKLIST.md` record what real-data runs found and fixed.

## Further reading

* `F1_Data_Engineering_Handbook.md` - the complete build-from-scratch handbook
* `docs/ANALYTICS_GUIDE.md` - models, assumptions and verification status
* `docs/example_queries.sql` - example analyst queries
