"""
Training function — called by the train DAG for each model config.

Receives your training rows and eval rows, trains a model,
evaluates on a held-out validation split, and returns predictions
on the eval rows for submission.

Edit points:
  1. Uncomment random_forest or gradient_boosting below
  2. Add derived features in build_features()
  3. Adjust or add to FEATURE_COLS
"""

import functions_framework
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


FEATURE_COLS = [
    "x",
    "y",
]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the feature matrix from a gold_shots DataFrame.
    Add new columns here — the same logic runs on both training and eval rows.
    """
    out = df[FEATURE_COLS].copy()

    # ----------------------------------------------------------------
    # Add your own features here. Examples:
    #   out["is_three"] = (df["shot_value"] == 3).astype(int)
    #   out["is_clutch"] = ((df["period"] == 4) & (df["clock_seconds"] < 120)).astype(int)
    #   out["dist_squared"] = df["shot_distance"] ** 2
    # ----------------------------------------------------------------

    return out.fillna(0)


@functions_framework.http
def task(request):
    body = request.get_json(silent=True) or {}

    config = body.get("config", {})
    train_records = body.get("train", [])
    eval_records = body.get("eval", [])

    algorithm = config.get("algorithm", "logistic_regression")
    hyperparams = config.get("hyperparameters", {})

    if not train_records:
        return {"error": "no training data provided"}, 400
    if not eval_records:
        return {"error": "no eval data provided"}, 400

    train_df = pd.DataFrame(train_records)
    eval_df = pd.DataFrame(eval_records)

    # 1 = Made, 0 = Missed
    train_df["label"] = (train_df["shot_result"] == "Made").astype(int)

    X_all = build_features(train_df)
    y_all = train_df["label"]

    X_train, X_val, y_train, y_val = train_test_split(
        X_all, y_all, test_size=0.2, random_state=42
    )

    # ------------------------------------------------------------------
    # The algorithm name must match what you set in MODEL_CONFIGS in train.py.
    # hyperparams is the dict you passed under "hyperparameters" — it gets
    # unpacked directly into the sklearn constructor, so {"n_estimators": 100}
    # becomes RandomForestClassifier(n_estimators=100).
    # Add a matching elif block when you add a new algorithm to MODEL_CONFIGS.
    # ------------------------------------------------------------------
    if algorithm == "logistic_regression":
        model = LogisticRegression(**hyperparams, random_state=42, max_iter=1000)

    # elif algorithm == "random_forest":
    #     model = RandomForestClassifier(**hyperparams, random_state=42)

    # elif algorithm == "gradient_boosting":
    #     model = GradientBoostingClassifier(**hyperparams, random_state=42)

    else:
        return {"error": f"unknown algorithm: {algorithm}"}, 400

    model.fit(X_train, y_train)

    val_preds = model.predict(X_val)
    accuracy = float(accuracy_score(y_val, val_preds))

    print(f"algorithm={algorithm} | val_accuracy={accuracy:.4f} | "
          f"train={len(X_train)} | val={len(X_val)}")

    # Generate predictions on the hidden eval rows
    X_eval = build_features(eval_df)
    eval_preds = model.predict(X_eval).tolist()

    predictions = [
        {
            "gameId": row["game_id"],
            "actionNumber": int(row["action_number"]),
            "made": int(pred),
        }
        for row, pred in zip(eval_records, eval_preds)
    ]

    return {
        "config": config,
        "metrics": {"accuracy": round(accuracy, 4)},
        "predictions": predictions,
    }, 200
