"""
Posts your best model's predictions to the leaderboard.

Triggered automatically by the train DAG when your model clears the threshold.
You should not need to edit this DAG.
"""

import json
import os
from datetime import datetime

import requests
from airflow.decorators import dag, task

API_BASE = os.environ.get("API_BASE", "https://btibert-bu--ba882-nba-ship-it")
TEAM_NUMBER = os.environ.get("TEAM_NUMBER", "0")


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nba", "submit"],
)
def submit_dag():

    @task()
    def submit(**context):
        conf = context["dag_run"].conf or {}
        best = conf.get("best", {})

        if isinstance(best, str):
            best = json.loads(best)

        predictions = best.get("predictions", [])
        if not predictions:
            raise ValueError("No predictions received — check that the train DAG completed successfully")

        payload = {
            "team": TEAM_NUMBER,
            "predictions": predictions,
        }

        url = f"{API_BASE}-submit.us-east.modal.run"
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        print(f"team={result.get('team')} | accuracy={result.get('accuracy')} | n={result.get('n')}")
        return result

    submit()


submit_dag()
