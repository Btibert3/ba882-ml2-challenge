# BA882 — NBA Shot Challenge

Predict whether an NBA shot goes in. Iterate on features and models to climb the leaderboard.

The raw data is already loaded in `nba.duckdb` — no ingestion step needed.
Your job is to build features, train models, and submit predictions.

---

## Setup

**1. Set your GCP project**
```bash
gcloud config set project YOUR_PROJECT_ID
```

**2. Clone the repo**
```bash
git clone https://github.com/btibert3/ba882-ml2-challenge.git
```

```bash
cd ba882-ml2-challenge
```

**3. Install the Astro CLI**
```bash
bash setup/install_astro.sh
```

**4. Configure your environment**
```bash
cp .env.example .env
```

Open `.env` and set your team number.

```bash
echo "DUCKDB_PATH=$(pwd)/nba.duckdb" >> .env
```

```bash
echo "DBT_EXECUTABLE_PATH=$(pwd)/.venv/bin/dbt" >> .env
```

**5. Start Airflow**
```bash
astro dev start
```

Open the web preview on port 8080. Log in with `admin` / `admin`.

---

## Running the pipeline

Trigger DAGs in this order from the Airflow UI:

1. **`dbt_dag`** — builds your models from the raw data (~30 seconds)
2. **`train`** — trains your models, auto-triggers submission if the model clears the threshold

That's it. Re-trigger `dbt_dag` after editing your features, then re-trigger `train` to submit again.

---

## Where to make changes

| File | What to do |
|---|---|
| `include/dbt/models/intermediate/int_shots.sql` | Add features |
| `dags/train.py` | Add `build_features()` logic, uncomment models, add configs to `MODEL_CONFIGS` |

---

## Why no ingestion DAG?

The raw play-by-play data is fixed for this exercise — everyone needs the same starting point
and the data doesn't change during class. Shipping `nba.duckdb` directly means you can focus
on the ML pipeline rather than data ingestion. In production you'd have an ingestion step;
we built one earlier this semester.
