"""
OPTIONAL: render the dbt project as individual Airflow tasks with Astronomer Cosmos
(one task per model, so a failed model/test shows up in the Airflow graph, not as one opaque "dbt build").

NOT exercised in this repo's tests. Needs `astronomer-cosmos` in the Airflow image, e.g. add it to
AIRFLOW_EXTRA_PIP in airflow/.env. If Cosmos is not installed this file defines nothing and parses cleanly.
"""
import logging
import os
from datetime import datetime

try:
    from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
except ImportError:  # keep the scheduler healthy when Cosmos is not installed
    logging.getLogger(__name__).info("astronomer-cosmos not installed; skipping dbt_cosmos_dag.")
else:
    PROJECT = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/f1_data_project")

    dbt_cosmos_dag = DbtDag(
        dag_id="dbt_f1_transformations_cosmos",
        project_config=ProjectConfig(PROJECT),
        profile_config=ProfileConfig(
            profile_name="dbt_f1_transformations",
            target_name=os.environ.get("DBT_TARGET", "container"),
            profiles_yml_filepath=os.path.join(PROJECT, "profiles.yml"),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=os.environ.get("DBT_EXECUTABLE", "dbt")),
        # fct_telemetry* are heavy and incremental: run them explicitly, not on every Cosmos run
        render_config=RenderConfig(exclude=["fct_telemetry", "fct_telemetry_distance_trace", "fct_telemetry_lap_summary", "fct_lap_time_delta_trace"]),
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["dbt", "cosmos"],
    )
