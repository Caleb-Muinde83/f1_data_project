from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='extract_openf1_data',
    default_args=default_args,
    description='Extracts 18 endpoints from the OpenF1 API using a Universal Extractor',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['openf1', 'extraction'],
)
def extract_openf1_dag():
    
    @task
    def start_extraction():
        logging.info("Starting OpenF1 API extraction pipeline...")
        return "Started"

    # --- UPDATED TO USE api_params ---
    @task
    def extract_endpoint_data(endpoint: str, api_params: dict = None, **kwargs):
        from modules.universal_extractor import extract_and_save_endpoint
        return extract_and_save_endpoint(endpoint=endpoint, api_params=api_params, **kwargs)

    with TaskGroup("event_and_competitor_data") as event_group:
        extract_endpoint_data.override(task_id="extract_meetings")("meetings")
        extract_endpoint_data.override(task_id="extract_sessions")("sessions")
        # FIX: The drivers endpoint requires a session_key or meeting_key, NOT a year!
        extract_endpoint_data.override(task_id="extract_drivers")("drivers", api_params={"session_key": 9158})

    with TaskGroup("telemetry_and_performance") as telemetry_group:
        # Pass a specific session_key (9158 = 2023 Abu Dhabi GP Race) for heavy data
        session_filter = {"session_key": 9158}
        
        extract_endpoint_data.override(task_id="extract_car_data")("car_data", api_params={"session_key": 9158, "driver_number": 1})
        extract_endpoint_data.override(task_id="extract_location")("location", api_params={"session_key": 9158, "driver_number": 1})
        extract_endpoint_data.override(task_id="extract_laps")("laps", api_params=session_filter)
        extract_endpoint_data.override(task_id="extract_pit")("pit", api_params=session_filter)

    with TaskGroup("race_results_and_events") as results_group:
        # Session Results also require a session_key to avoid data overload
        extract_endpoint_data.override(task_id="extract_session_result")("session_result", api_params={"session_key": 9158})
        extract_endpoint_data.override(task_id="extract_race_control")("race_control", api_params={"session_key": 9158})
        extract_endpoint_data.override(task_id="extract_weather")("weather", api_params={"session_key": 9158})

    @task
    def verify_and_load_to_gcs():
        logging.info("All data extracted locally. Ready to push to GCS!")
        return "Finished"

    start = start_extraction()
    end = verify_and_load_to_gcs()

    start >> event_group >> [telemetry_group, results_group] >> end

dag_instance = extract_openf1_dag()