"""
Trains models in parallel, picks the best one, and submits predictions.

Trigger this manually after:
  1. Editing and deploying your Cloud Function (bash functions/deploy.sh)
  2. Running dbt (trigger dbt_dag, or re-trigger ingest)

Flow:
  load_data
    └── .expand() over MODEL_CONFIGS ──► call Cloud Function (one task per config)
          └── pick_best
                └── @task.branch ──► good enough to submit?
                      ├── YES ──► triggers submit_dag
                      └── NO  ──► skip

Edit points:
  1. Add more configs to MODEL_CONFIGS to run more experiments in parallel
  2. Adjust DEPLOY_THRESHOLD — how good does the model need to be before submitting?
"""

import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str((Path(__file__).parent / "../nba.duckdb").resolve()))
TRAIN_FUNCTION_URL = os.environ.get("TRAIN_FUNCTION_URL", "")

# Minimum validation accuracy before predictions are submitted
DEPLOY_THRESHOLD = 0.55

# -----------------------------------------------------------------------
# Each entry here becomes one parallel task in the Airflow graph.
# "algorithm" must match a block in functions/nba-train/main.py.
# "hyperparameters" are passed directly to the sklearn model constructor —
# for example, n_estimators=100 is the same as RandomForestClassifier(n_estimators=100).
# Uncomment rows to run more experiments, or add your own.
# -----------------------------------------------------------------------
MODEL_CONFIGS = [
    {"algorithm": "logistic_regression", "hyperparameters": {"max_iter": 1000}},
    # {"algorithm": "random_forest",       "hyperparameters": {"n_estimators": 100}},
    # {"algorithm": "gradient_boosting",   "hyperparameters": {"n_estimators": 100, "learning_rate": 0.1}},
]


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nba", "train"],
)
def train():

    @task()
    def load_data():
        con = duckdb.connect(DUCKDB_PATH, read_only=True)

        # Rows where the outcome is known — used for training
        train_df = con.execute(
            "SELECT * FROM gold_shots WHERE shot_result IS NOT NULL"
        ).df()

        # Rows where the outcome is hidden — your predictions on these go to the leaderboard
        eval_df = con.execute(
            "SELECT * FROM gold_shots WHERE shot_result IS NULL"
        ).df()

        con.close()
        print(f"train rows: {len(train_df)} | eval rows: {len(eval_df)}")

        return {
            "train": train_df.to_dict(orient="records"),
            "eval": eval_df.to_dict(orient="records"),
        }

    @task()
    def run_experiment(data: dict, config: dict):
        """Call the Cloud Function with one model config."""
        if not TRAIN_FUNCTION_URL:
            raise ValueError("TRAIN_FUNCTION_URL is not set — add it to your .env file")

        payload = {
            "config": config,
            "train": data["train"],
            "eval": data["eval"],
        }
        resp = requests.post(TRAIN_FUNCTION_URL, json=payload, timeout=120)
        resp.raise_for_status()
        result = resp.json()
        print(f"config={config['algorithm']} | accuracy={result.get('metrics', {}).get('accuracy')}")
        return result

    @task()
    def pick_best(results: list):
        """Choose the experiment with the highest validation accuracy."""
        best = max(results, key=lambda r: r.get("metrics", {}).get("accuracy", 0))
        print(f"best: {best.get('config', {}).get('algorithm')} | "
              f"accuracy={best.get('metrics', {}).get('accuracy')}")
        return best

    @task.branch()
    def should_deploy(best: dict):
        accuracy = best.get("metrics", {}).get("accuracy", 0)
        if accuracy >= DEPLOY_THRESHOLD:
            print(f"accuracy {accuracy} >= threshold {DEPLOY_THRESHOLD} — submitting")
            return "trigger_submit"
        else:
            print(f"accuracy {accuracy} < threshold {DEPLOY_THRESHOLD} — skipping")
            return "skip"

    trigger_submit = TriggerDagRunOperator(
        task_id="trigger_submit",
        trigger_dag_id="submit_dag",
        conf={"best": "{{ ti.xcom_pull(task_ids='pick_best') }}"},
        wait_for_completion=False,
    )

    skip = EmptyOperator(task_id="skip")

    data = load_data()
    results = run_experiment.expand(config=MODEL_CONFIGS)(data=data)
    best = pick_best(results)
    branch = should_deploy(best)
    branch >> [trigger_submit, skip]


train()
