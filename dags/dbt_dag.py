"""
Runs the dbt project using Cosmos.

This DAG is triggered automatically when ingestion completes.
You can also trigger it manually after editing your dbt models —
no need to re-run ingestion if the raw data hasn't changed.

Flow: stg_teams, stg_games, stg_plays → int_shots → gold_shots
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode

DBT_PROJECT_PATH = (Path(__file__).parent / "../include/dbt").resolve()
DBT_PROFILES_PATH = DBT_PROJECT_PATH

# On Cloud Shell, dbt is installed at ~/.local/bin/dbt
# Set DBT_EXECUTABLE_PATH in your .env if the default doesn't match:
#   echo "DBT_EXECUTABLE_PATH=/home/$(whoami)/.local/bin/dbt" >> .env
DBT_EXECUTABLE_PATH = os.environ.get(
    "DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt"
)

dbt_dag = DbtDag(
    dag_id="dbt_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nba", "dbt"],
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="nba_challenge",
        target_name="dev",
        profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
)
