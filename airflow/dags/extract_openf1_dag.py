import json
import logging
import os
import shlex
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from modules.universal_extractor import extract_and_save_endpoint
from modules.pipeline_utils import build_manifest, notify_failure
from hooks.openf1_hook import OpenF1Hook

# ==========================================
# DBT LOCATIONS (inside the Airflow container; see docker-compose.yaml for the mounts)
# ==========================================
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/f1_data_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_TARGET = os.environ.get("DBT_TARGET", "container")
RAW_DIR = os.environ.get("OPENF1_OUTPUT_PATH", "/opt/airflow/data/raw")

# ==========================================
# DEFAULT ARGS
# ==========================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,      # 3 min, 6 min, ... so a rate-limited API gets time to recover
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),  # a hung HTTP call must not hold a pool slot forever
    "on_failure_callback": notify_failure,    # structured log line + optional webhook (ALERT_WEBHOOK_URL)
}

# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id="extract_openf1_data",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "target_session_key": Param(
            default="latest", type="string",
            description="Enter a session_key, 'latest', or 'all'.",
        ),
        "min_session_key": Param(
            default=0, type="integer",
            description="Only extract sessions with session_key >= this value (use with 'all' to bound a backfill).",
        ),
        "max_sessions": Param(
            default=150, type="integer",
            description="Fan-out guard: abort if more sessions than this would be extracted.",
        ),
        "allow_large_backfill": Param(
            default=False, type="boolean",
            description="Tick to override the max_sessions guard on purpose.",
        ),
        "skip_existing": Param(
            default=False, type="boolean",
            description="Do not re-download endpoint/session files that already exist (cheap backfills; never use for a live session).",
        ),
        "run_dbt_build": Param(
            default=False, type="boolean",
            description="Run `dbt build` after extraction (needs dbt + the dbt project mounted in the container).",
        ),
        "transcribe_radio": Param(
            default=False, type="boolean",
            description="Transcribe team radio audio with faster-whisper (needs the package + outbound HTTPS; slow on CPU).",
        ),
        "upload_to_gcs": Param(
            default=False, type="boolean",
            description="Upload extracted Parquet to GCS_RAW_BUCKET after extraction.",
        ),
    },
    tags=["openf1", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start_extraction")

    # ==========================================
    # TASK: FETCH SESSION METADATA (+ fan-out guard)
    # ==========================================
    @task
    def fetch_session_metadata(**context):
        """Fetches the raw session metadata from the API using the custom Hook, then applies the backfill guard."""
        hook = OpenF1Hook()
        p = context["params"]
        target = p["target_session_key"]

        if str(target).lower() == "all":
            params = {}  # omit session_key entirely: API returns full history + future
        elif str(target).lower() == "latest":
            params = {"session_key": "latest"}
        else:
            params = {"session_key": int(target) if str(target).isdigit() else target}

        logging.info(f"Fetching session metadata for: {params if params else 'ALL historical sessions'}")
        sessions = hook.get_endpoint('/sessions', params=params)

        if not sessions:
            logging.warning("No sessions found for the given parameter.")
            return []

        min_key = int(p["min_session_key"])
        if min_key:
            sessions = [s for s in sessions if int(s["session_key"]) >= min_key]

        if len(sessions) > int(p["max_sessions"]) and not p["allow_large_backfill"]:
            raise AirflowFailException(
                f"{len(sessions)} sessions would be extracted (guard max_sessions={p['max_sessions']}). "
                "Narrow the range with min_session_key, or tick allow_large_backfill to proceed deliberately."
            )
        logging.info(f"{len(sessions)} session(s) selected for extraction.")
        return sessions

    # ==========================================
    # TASKS: FILTER SESSIONS FOR DYNAMIC MAPPING
    # ==========================================
    @task
    def get_all_keys(sessions):
        return [s['session_key'] for s in sessions]

    @task
    def get_race_keys(sessions):
        return [s['session_key'] for s in sessions if s.get('session_type') == 'Race']

    @task
    def get_quali_and_race_keys(sessions):
        valid_types = {'Race', 'Qualifying', 'Sprint', 'Sprint Qualifying'}
        return [s['session_key'] for s in sessions if s.get('session_type') in valid_types]

    # ==========================================
    # TASK: EXTRACTION WRAPPER (Mapped)
    # ==========================================
    @task(pool='openf1_api_pool')
    def extract_mapped_data(endpoint: str, target_session: str, is_global: bool = False, **context):
        return extract_and_save_endpoint(
            endpoint=endpoint,
            target_session=target_session,
            is_global=is_global,
            skip_existing=bool(context["params"]["skip_existing"]),
            run_id=context["run_id"],
        )

    # ==========================================
    # WORKFLOW DEFINITION
    # ==========================================
    raw_sessions = fetch_session_metadata()
    all_sessions = get_all_keys(raw_sessions)
    race_sessions = get_race_keys(raw_sessions)
    quali_race_sessions = get_quali_and_race_keys(raw_sessions)

    with TaskGroup("global_and_reference") as event_group:
        global_endpoints = ["meetings", "drivers", "sessions"]
        event_results = []
        for ep in global_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep, is_global=True
            ).expand(target_session=["global"])
            event_results.append(res)

    with TaskGroup("telemetry_and_performance") as telemetry_group:
        telemetry_endpoints = ["car_data", "location"]
        telemetry_results = []
        for ep in telemetry_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            telemetry_results.append(res)

    with TaskGroup("race_results_and_events") as results_group:
        results_out = []

        all_session_endpoints = [
            "weather", "team_radio", "race_control", "pit",
            "laps", "stints", "position", "session_result"
        ]
        for ep in all_session_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_all").partial(endpoint=ep).expand(target_session=all_sessions)
            results_out.append(res)

        race_endpoints = ["intervals", "overtakes", "championship_drivers", "championship_teams"]
        for ep in race_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_race").partial(endpoint=ep).expand(target_session=race_sessions)
            results_out.append(res)

        res_grid = extract_mapped_data.override(task_id="extract_starting_grid").partial(endpoint="starting_grid").expand(target_session=quali_race_sessions)
        results_out.append(res_grid)

    # ==========================================
    # TASK: VERIFY, RECONCILE AND WRITE A MANIFEST
    # ==========================================
    @task
    def verify_and_load(event_results, telemetry_results, results_out, **context):
        flat_results = []
        for task_group_results in [event_results, telemetry_results, results_out]:
            for item in task_group_results:
                if isinstance(item, list):
                    flat_results.extend(item)
                else:
                    flat_results.append(item)

        saved_files = [r for r in flat_results if r is not None]

        if not saved_files:
            logging.warning("Pipeline completed, but NO files were successfully extracted or saved.")
            return {"files": [], "manifest_path": None}

        manifest_path = build_manifest(saved_files, RAW_DIR, context["run_id"])
        logging.info(f"{len(saved_files)} Parquet files extracted. Manifest written to {manifest_path}. Ready for dbt!")
        return {"files": saved_files, "manifest_path": manifest_path}

    load_task = verify_and_load(event_results, telemetry_results, results_out)

    # ==========================================
    # OPTIONAL: UPLOAD RAW FILES TO GCS
    # ==========================================
    @task
    def upload_raw_to_gcs(load_result, **context):
        if not context["params"]["upload_to_gcs"]:
            raise AirflowSkipException("upload_to_gcs is off for this run.")
        bucket = os.environ.get("GCS_RAW_BUCKET")
        if not bucket:
            raise AirflowFailException("upload_to_gcs=True but env var GCS_RAW_BUCKET is not set.")
        from modules.gcs_loader import upload_raw_files
        return upload_raw_files(load_result["files"], bucket=bucket)

    # ==========================================
    # OPTIONAL: RUN DBT AFTER EXTRACTION
    # ==========================================
    @task
    def run_dbt_build(load_result, **context):
        if not context["params"]["run_dbt_build"]:
            raise AirflowSkipException("run_dbt_build is off for this run.")
        if not load_result["files"]:
            raise AirflowSkipException("Nothing was extracted, so there is nothing to transform.")

        env = {**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR}
        cmd = f"dbt build --target {shlex.quote(DBT_TARGET)} --profiles-dir {shlex.quote(DBT_PROFILES_DIR)}"
        logging.info(f"Running: {cmd} (cwd={DBT_PROJECT_DIR})")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=DBT_PROJECT_DIR, env=env)
        logging.info(result.stdout[-6000:])
        if result.returncode != 0:
            logging.error(result.stderr[-3000:])
            raise AirflowFailException(f"dbt build failed with exit code {result.returncode}. See the task log above.")
        return "dbt build succeeded"

    # ==========================================
    # OPTIONAL: TRANSCRIBE TEAM RADIO (one session at a time: CPU heavy)
    # ==========================================
    @task(max_active_tis_per_dag=1)
    def transcribe_team_radio(target_session, **context):
        if not context["params"]["transcribe_radio"]:
            raise AirflowSkipException("transcribe_radio is off for this run.")
        from modules.radio_transcriber import transcribe_session
        return transcribe_session(target_session, RAW_DIR, model_name=os.environ.get("RADIO_WHISPER_MODEL", "small.en"))

    radio_task = transcribe_team_radio.expand(target_session=all_sessions)

    upload_task = upload_raw_to_gcs(load_task)
    dbt_task = run_dbt_build(load_task)

    # ==========================================
    # EXPLICIT DEPENDENCY GRAPH
    # ==========================================
    start >> raw_sessions >> [all_sessions, race_sessions, quali_race_sessions]
    start >> event_group
    all_sessions >> telemetry_group
    [all_sessions, race_sessions, quali_race_sessions] >> results_group
    [event_group, telemetry_group, results_group] >> load_task
    results_group >> radio_task
