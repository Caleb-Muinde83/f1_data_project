from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'pool': 'openf1_api_pool', # Throttles concurrency to max 2 tasks at a time
}

@dag(
    dag_id='extract_openf1_data',
    default_args=default_args,
    description='Extracts all 18 endpoints from the OpenF1 API using a Universal Extractor',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['openf1', 'extraction'],
)
def extract_openf1_dag():
    
    @task
    def start_extraction():
        logging.info("Starting OpenF1 API full extraction pipeline...")
        return "Started"

    # --- UNIVERSAL EXTRACTOR TASK ---
    @task
    def extract_endpoint_data(endpoint: str, api_params: dict = None, **kwargs):
        from modules.universal_extractor import extract_and_save_endpoint
        return extract_and_save_endpoint(endpoint=endpoint, api_params=api_params, **kwargs)

    # --- 1. EVENT AND COMPETITOR DATA (5 Endpoints) ---
    with TaskGroup("event_and_competitor_data") as event_group:
        extract_endpoint_data.override(task_id="extract_meetings")("meetings")
        extract_endpoint_data.override(task_id="extract_sessions")("sessions")
        extract_endpoint_data.override(task_id="extract_drivers")("drivers", api_params={"session_key": 9158})
        # FIX: Beta endpoints lack older 2023 historical data. Use "latest" instead.
        extract_endpoint_data.override(task_id="extract_championship_drivers")("championship_drivers", api_params={"session_key": "latest"})
        extract_endpoint_data.override(task_id="extract_championship_teams")("championship_teams", api_params={"session_key": "latest"})

    # --- 2. TELEMETRY AND PERFORMANCE (8 Endpoints) ---
    with TaskGroup("telemetry_and_performance") as telemetry_group:
        session_filter = {"session_key": 9158}
        
        # Heavy telemetry restricted to Driver 1 (Max Verstappen) to prevent 422 errors
        extract_endpoint_data.override(task_id="extract_car_data")("car_data", api_params={"session_key": 9158, "driver_number": 1})
        extract_endpoint_data.override(task_id="extract_location")("location", api_params={"session_key": 9158, "driver_number": 1})
        
        # Standard performance endpoints
        extract_endpoint_data.override(task_id="extract_laps")("laps", api_params=session_filter)
        extract_endpoint_data.override(task_id="extract_pit")("pit", api_params=session_filter)
        extract_endpoint_data.override(task_id="extract_stints")("stints", api_params=session_filter)
        extract_endpoint_data.override(task_id="extract_intervals")("intervals", api_params={"session_key": "latest"})
        extract_endpoint_data.override(task_id="extract_position")("position", api_params=session_filter)
        extract_endpoint_data.override(task_id="extract_overtakes")("overtakes", api_params={"session_key": "latest"})

    # --- 3. RACE RESULTS AND EVENTS (5 Endpoints) ---
    with TaskGroup("race_results_and_events") as results_group:
        extract_endpoint_data.override(task_id="extract_starting_grid")("starting_grid", api_params={"session_key": 7783})
        extract_endpoint_data.override(task_id="extract_session_result")("session_result", api_params={"session_key": 9158})
        extract_endpoint_data.override(task_id="extract_race_control")("race_control", api_params={"session_key": 9158})
        extract_endpoint_data.override(task_id="extract_team_radio")("team_radio", api_params={"session_key": 9158})
        extract_endpoint_data.override(task_id="extract_weather")("weather", api_params={"session_key": 9158})

    @task
    def verify_and_load_to_gcs():
        logging.info("All 18 endpoints extracted locally. Ready to push to GCS!")
        return "Finished"

    # --- DEFINE DEPENDENCIES ---
    start = start_extraction()
    end = verify_and_load_to_gcs()

    start >> event_group >> [telemetry_group, results_group] >> end

# Instantiate the DAG
dag_instance = extract_openf1_dag()