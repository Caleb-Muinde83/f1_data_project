import os
import shlex
import logging
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException

# Import your refactored extractor module and hook
from modules.universal_extractor import extract_and_save_endpoint
from hooks.openf1_hook import OpenF1Hook

# ==========================================
# PROJECT PATHS
# ==========================================
DBT_PROJECT_DIR = "/opt/airflow/dbt/f1_data_project"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/.dbt")

# ==========================================
# DEFAULT ARGS
# ==========================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
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
            default="latest",
            type="string",
            description="Enter a session_key integer, 'latest', or 'all'."
        )
    },
    tags=["openf1", "ingestion", "dbt"]
) as dag:

    start = EmptyOperator(task_id="start_extraction")

    # ==========================================
    # TASK: FETCH SESSION METADATA
    # ==========================================
    @task
    def fetch_session_metadata(**context):
        """Fetches raw session metadata from the OpenF1 API using the custom Hook."""
        hook = OpenF1Hook()
        target = context["params"]["target_session_key"]

        if str(target).lower() == "all":
            params = {}  # No filter — API returns full history
        elif str(target).lower() == "latest":
            params = {"session_key": "latest"}
        else:
            params = {"session_key": int(target) if str(target).isdigit() else target}

        logging.info(
            f"Fetching session metadata for: "
            f"{params if params else 'ALL historical sessions'}"
        )

        sessions = hook.get_endpoint("/sessions", params=params)

        if not sessions:
            logging.warning("No sessions found for the given parameter.")
            return []

        # ==========================================
        # FAN-OUT GUARD: Protect worker from massive 'all' payload
        # ==========================================
        if str(target).lower() == "all":
            # OpenF1 session keys are sequential integers; 9000+ corresponds to ~2023 onward
            filtered_sessions = [s for s in sessions if s.get("session_key", 0) >= 9000]
            logging.warning(
                f"Historical load requested. Capping {len(sessions)} total sessions "
                f"to {len(filtered_sessions)} (ID >= 9000) to prevent worker OOM and API limits."
            )
            return filtered_sessions

        return sessions

    # ==========================================
    # TASKS: FILTER SESSIONS FOR DYNAMIC MAPPING
    # ==========================================
    @task
    def get_all_keys(sessions):
        """Returns session_key for every session."""
        return [s["session_key"] for s in sessions]

    @task
    def get_race_keys(sessions):
        """Returns session_key for Race sessions only."""
        return [s["session_key"] for s in sessions if s.get("session_type") == "Race"]

    @task
    def get_quali_and_race_keys(sessions):
        """Returns session_key for Race, Qualifying, Sprint, and Sprint Qualifying sessions."""
        valid_types = {"Race", "Qualifying", "Sprint", "Sprint Qualifying"}
        return [
            s["session_key"] for s in sessions
            if s.get("session_type") in valid_types
        ]

    # ==========================================
    # TASK: EXTRACTION WRAPPER (Dynamically Mapped)
    # ==========================================
    @task(pool="openf1_api_pool")
    def extract_mapped_data(endpoint: str, target_session: str, is_global: bool = False):
        """Universal extraction task — mapped over session keys per endpoint."""
        return extract_and_save_endpoint(
            endpoint=endpoint,
            target_session=target_session,
            is_global=is_global
        )

    # ==========================================
    # WORKFLOW: RESOLVE SESSIONS
    # ==========================================

    # Call resolve tasks EXACTLY ONCE each — XComArgs are reused across groups
    raw_sessions      = fetch_session_metadata()
    all_sessions      = get_all_keys(raw_sessions)
    race_sessions     = get_race_keys(raw_sessions)
    quali_race_sessions = get_quali_and_race_keys(raw_sessions)

    # ==========================================
    # TASK GROUP 1: GLOBAL / REFERENCE DATA
    # Endpoints that don't vary by session — extracted once.
    # ==========================================
    with TaskGroup("global_and_reference") as event_group:
        global_endpoints = ["meetings", "drivers", "sessions"]
        event_results = []
        for ep in global_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep, is_global=True
            ).expand(target_session=["global"])
            event_results.append(res)

    # ==========================================
    # TASK GROUP 2: HEAVY TELEMETRY
    # Mapped to ALL sessions — highest fan-out, pool-throttled.
    # ==========================================
    with TaskGroup("telemetry_and_performance") as telemetry_group:
        telemetry_endpoints = ["car_data", "location"]
        telemetry_results = []
        for ep in telemetry_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            telemetry_results.append(res)

    # ==========================================
    # TASK GROUP 3: RACE RESULTS & EVENTS
    # Each endpoint mapped only to the sessions where its data exists.
    # ==========================================
    with TaskGroup("race_results_and_events") as results_group:
        results_out = []

        # All sessions
        all_session_endpoints = [
            "weather", "team_radio", "race_control", "pit",
            "laps", "stints", "position", "session_result"
        ]
        for ep in all_session_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_all").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            results_out.append(res)

        # Race sessions only
        race_endpoints = [
            "intervals", "overtakes", "championship_drivers", "championship_teams"
        ]
        for ep in race_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_race").partial(
                endpoint=ep
            ).expand(target_session=race_sessions)
            results_out.append(res)

        # Qualifying + Race + Sprint sessions only
        res_grid = extract_mapped_data.override(task_id="extract_starting_grid").partial(
            endpoint="starting_grid"
        ).expand(target_session=quali_race_sessions)
        results_out.append(res_grid)

    # ==========================================
    # TASK: VERIFY AND LOAD
    # Aggregates results from all groups before triggering dbt.
    # ==========================================
    @task
    def verify_and_load(event_results_in, telemetry_results_in, results_out_in):
        """Flattens and counts successfully extracted files across all task groups."""
        flat_results = []
        for group in [event_results_in, telemetry_results_in, results_out_in]:
            for item in group:
                if isinstance(item, list):
                    flat_results.extend(item)
                else:
                    flat_results.append(item)

        saved_files = [r for r in flat_results if r is not None]

        if not saved_files:
            logging.warning(
                "Pipeline completed, but NO files were successfully extracted or saved."
            )
        else:
            logging.info(
                f"✅ Success! {len(saved_files)} Parquet files extracted and saved. "
                f"Triggering dbt..."
            )

    load_task = verify_and_load(event_results, telemetry_results, results_out)

    # ==========================================
    # TASK: DBT TRANSFORMATION & EXPORT
    # Receives all_sessions XComArg — same single reference used throughout.
    # ==========================================
    @task
    def run_dbt_telemetry(session_keys_list: list):
        """
        Runs dbt incrementally against fct_telemetry, then exports to Parquet
        via the export_fct_telemetry macro.

        Incremental logic lives in the SQL model (NOT EXISTS against {{ this }}),
        so no --vars are needed for the build step.
        --vars are passed only to the run-operation export macro.
        """
        if not session_keys_list:
            logging.info("No session keys to process. Skipping dbt.")
            return

        env = {**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR}

        def run_cmd(cmd: str):
            logging.info(f"Executing: {cmd}")
            result = subprocess.run(
                cmd, shell=True, capture_output=True,
                text=True, cwd=DBT_PROJECT_DIR, env=env
            )
            if result.returncode != 0:
                raise AirflowException(
                    f"Command failed:\n{result.stderr}\n{result.stdout}"
                )
            logging.info(result.stdout)

        # Step 1: Incremental build — model uses NOT EXISTS against {{ this }},
        # so it automatically skips sessions already present in the table.
        run_cmd("dbt run --select fct_telemetry")

        # Step 2: Export via macro — vars determine session scope for the COPY statement.
        if len(session_keys_list) == 1:
            vars_json = (
                f'{{"target_session_key": {session_keys_list[0]}, "export_data": true}}'
            )
            logging.info(f"Incremental Parquet export for session: {session_keys_list[0]}")
        else:
            vars_json = '{"export_data": true}'
            logging.info(
                f"Full historical Parquet export for {len(session_keys_list)} sessions."
            )

        run_cmd(
            f"dbt run-operation export_fct_telemetry --vars {shlex.quote(vars_json)}"
        )
        logging.info("✅ dbt transformation and export completed successfully.")

    dbt_task = run_dbt_telemetry(all_sessions)

    # ==========================================
    # EXPLICIT DEPENDENCY GRAPH
    # ==========================================

    # Start -> fetch metadata -> derive filter lists
    start >> raw_sessions >> [all_sessions, race_sessions, quali_race_sessions]

    # Global extractions have no session dependency — run immediately after start
    start >> event_group

    # Each group waits only on the filter(s) it actually uses
    all_sessions >> telemetry_group
    [all_sessions, race_sessions, quali_race_sessions] >> results_group

    # All groups must complete before load aggregation
    [event_group, telemetry_group, results_group] >> load_task

    # dbt runs after load is confirmed; receives all_sessions for export scoping
    load_task >> dbt_task