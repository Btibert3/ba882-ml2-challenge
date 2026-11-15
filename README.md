# BA882 — NBA Shot Challenge

Predict whether an NBA shot goes in. Build a pipeline, train a model, climb the leaderboard.

---

## Setup

**1. Set your GCP project**
```bash
gcloud config set project YOUR_PROJECT_ID
```

**2. Clone the repo**
```bash
git clone https://github.com/btibert3/ba882-ml2-challenge.git
cd ba882-ml2-challenge
```

**3. Install the Astro CLI**
```bash
bash setup/install_astro.sh
```

**4. Install dbt**
```bash
pip install dbt-duckdb
```

**5. Configure your environment**
```bash
cp .env.example .env
```
Open `.env` and set your team number.

```bash
echo "DUCKDB_PATH=$(pwd)/nba.duckdb" >> .env
```

```bash
echo "DBT_EXECUTABLE_PATH=$(which dbt)" >> .env
```

**6. Deploy your Cloud Function**
```bash
bash functions/deploy.sh
```
Copy the URL it prints. Paste it into `.env` as `TRAIN_FUNCTION_URL`.

**7. Start Airflow**
```bash
astro dev start
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
