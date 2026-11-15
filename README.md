# BA882 — NBA Shot Challenge

Predict whether an NBA shot goes in. Build a pipeline, train a model, climb the leaderboard.

---

## Setup

**1. Install dependencies**
```bash
bash setup/install_astro.sh
pip install dbt-duckdb
```

**2. Configure your environment**

Open `.env` and update three values:
- `TEAM_NUMBER` — your team number
- `DUCKDB_PATH` — run `echo "DUCKDB_PATH=$(pwd)/nba.duckdb" >> .env`
- `DBT_EXECUTABLE_PATH` — run `which dbt` and paste the result

**3. Start Airflow**
```bash
astro dev start
```

**4. Deploy your Cloud Function**
```bash
bash functions/deploy.sh
```
Paste the URL into `.env` as `TRAIN_FUNCTION_URL`, then restart:
```bash
astro dev restart
```

---

## Running the pipeline

Trigger DAGs in this order from the Airflow UI:

1. **`ingest`** — loads raw data into DuckDB, then auto-triggers dbt
2. **`dbt_dag`** — builds your models (re-trigger this after editing dbt, no need to re-ingest)
3. **`train`** — trains your models, auto-triggers submission if the model clears the threshold

---

## Where to make changes

| File | What to do |
|---|---|
| `include/dbt/models/intermediate/int_shots.sql` | Add features |
| `functions/nba-train/main.py` | Uncomment models, add feature logic |
| `dags/train.py` | Add configs to `MODEL_CONFIGS`, adjust `DEPLOY_THRESHOLD` |

After editing the function, redeploy:
```bash
bash functions/deploy.sh
```

After editing dbt, re-trigger `dbt_dag` from the UI.
