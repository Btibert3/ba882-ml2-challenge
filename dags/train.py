"""
Trains models in parallel, picks the best one, and submits predictions.

Trigger this manually after running dbt_dag.

Flow:
  load_data
    └── .expand() over MODEL_CONFIGS ──► train_model (one task per config)
          └── pick_best
                └── @task.branch ──► good enough to submit?
                      ├── YES ──► triggers submit_dag
                      └── NO  ──► skip

Edit points:
  1. Add features in build_features() below
  2. Add more configs to MODEL_CONFIGS to run more experiments in parallel
  3. Adjust DEPLOY_THRESHOLD — how good does the model need to be before submitting?
"""

import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str((Path(__file__).parent / "../nba.duckdb").resolve()))

# Minimum validation accuracy before predictions are submitted
DEPLOY_THRESHOLD = 0.55

# -----------------------------------------------------------------------
# Each entry here becomes one parallel task in the Airflow graph.
# "algorithm" must match an elif block in train_model() below.
# "hyperparameters" are passed directly to the sklearn model constructor.
# Uncomment rows to run more experiments, or add your own.
# -----------------------------------------------------------------------
MODEL_CONFIGS = [
    {"algorithm": "logistic_regression", "hyperparameters": {"max_iter": 1000}},
    # {"algorithm": "random_forest",       "hyperparameters": {"n_estimators": 100}},
    # {"algorithm": "gradient_boosting",   "hyperparameters": {"n_estimators": 100, "learning_rate": 0.1}},
]

# Columns used to identify a shot for submission — never used as features
ID_COLS = ["game_id", "action_number"]

# The target column
TARGET = "shot_result"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Automatically uses every column in gold_shots except the IDs and target.
    To add features: add columns to gold_shots.sql in dbt, then re-run dbt_dag.
    """
    drop_cols = set(ID_COLS + [TARGET, "label"])
    feature_cols = [c for c in df.columns if c not in drop_cols]
    return df[feature_cols].fillna(0)


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
        train_df = con.execute(
            "SELECT * FROM gold_shots WHERE shot_result IS NOT NULL"
        ).df()
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
    def train_model(data: dict, config: dict):
        """Train one model config locally and return metrics + predictions."""
        algorithm = config.get("algorithm", "logistic_regression")
        hyperparams = config.get("hyperparameters", {})

        train_df = pd.DataFrame(data["train"])
        eval_df = pd.DataFrame(data["eval"])

        train_df["label"] = (train_df["shot_result"] == "Made").astype(int)

        X_all = build_features(train_df)
        y_all = train_df["label"]

        X_train, X_val, y_train, y_val = train_test_split(
            X_all, y_all, test_size=0.2, random_state=42
        )

        if algorithm == "logistic_regression":
            model = LogisticRegression(**hyperparams, random_state=42)

        elif algorithm == "random_forest":
            model = RandomForestClassifier(**hyperparams, random_state=42)

        elif algorithm == "gradient_boosting":
            model = GradientBoostingClassifier(**hyperparams, random_state=42)

        else:
            raise ValueError(f"unknown algorithm: {algorithm}")

        model.fit(X_train, y_train)

        accuracy = float(accuracy_score(y_val, model.predict(X_val)))
        print(f"algorithm={algorithm} | val_accuracy={accuracy:.4f} | "
              f"train={len(X_train)} | val={len(X_val)}")

        X_eval = build_features(eval_df)
        eval_preds = model.predict(X_eval).tolist()

        predictions = [
            {
                "gameId": row["game_id"],
                "actionNumber": int(row["action_number"]),
                "made": int(pred),
            }
            for row, pred in zip(data["eval"], eval_preds)
        ]

        return {
            "config": config,
            "metrics": {"accuracy": round(accuracy, 4)},
            "predictions": predictions,
        }

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

    @task()
    def skip():
        print("model did not clear threshold — skipping submission")

    data = load_data()
    results = train_model.partial(data=data).expand(config=MODEL_CONFIGS)
    best = pick_best(results)
    branch = should_deploy(best)
    branch >> [trigger_submit, skip()]


train()
