# BA882 — MLOps Orchestration Challenge

Predict whether an NBA shot goes in. Build features, train models, climb the leaderboard.

The raw play-by-play data is already in `nba.duckdb`. Your pipeline: **dbt → train → submit**.

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

## The loop

1. **`dbt_dag`** — builds your models (~30 seconds). Re-run this every time you edit dbt.
2. **`train`** — trains all your model configs in parallel, picks the best, auto-submits if it clears the threshold.

**Edit dbt → trigger `dbt_dag` → trigger `train` → check the leaderboard. Repeat.**

---

## Where to make changes

### 1. Add features — `include/dbt/models/intermediate/int_shots.sql`

This is your main workspace. The intermediate layer filters to field goal attempts and joins game context. Add derived columns here — anything you can compute from the raw data.

After editing, re-trigger `dbt_dag` to rebuild, then re-trigger `train`.

### 2. Promote features to gold — `include/dbt/models/gold/gold_shots.sql`

The gold layer is what the training DAG reads. It starts with just `x`, `y`, and the submission IDs. **Any column you add here automatically becomes a model feature** — no changes needed in the training code.

### 3. Try more models — `dags/train.py`

Uncomment configs in `MODEL_CONFIGS` to run multiple algorithms in parallel. Each becomes its own Airflow task. The best one gets submitted automatically.

```python
MODEL_CONFIGS = [
    {"algorithm": "logistic_regression", "hyperparameters": {"max_iter": 1000}},
    {"algorithm": "random_forest",       "hyperparameters": {"n_estimators": 100}},
    {"algorithm": "gradient_boosting",   "hyperparameters": {"n_estimators": 100, "learning_rate": 0.1}},
]
```

---

## Data dictionary

### Raw tables (read-only)

**`raw_teams`**

| Column | Description |
|---|---|
| `team_id` | Unique team ID |
| `full_name` | e.g. Boston Celtics |
| `abbreviation` | e.g. BOS |
| `city` | e.g. Boston |

**`raw_games`**

| Column | Description |
|---|---|
| `game_id` | Unique game ID |
| `game_date` | Date of the game |
| `home_team_id` / `home_team` | Home team |
| `away_team_id` / `away_team` | Away team |

**`raw_plays`** — every play event (not just shots)

| Column | Description |
|---|---|
| `gameId` | Game ID |
| `actionNumber` | Unique action within a game |
| `clock` | Time remaining in period (e.g. `PT11M32.00S`) |
| `period` | Quarter (1–4, 5+ for OT) |
| `teamId` / `teamTricode` | Team |
| `playerName` | Player |
| `xLegacy` / `yLegacy` | Court coordinates |
| `shotResult` | `Made` or `Missed` — **NULL for the eval set** |
| `isFieldGoal` | 1 if this is a shot attempt |
| `location` | `h` = home, `a` = away |
| `actionType` / `subType` | Shot classification |
| `shotDistance` | Distance from basket (feet) |
| `shotValue` | 2 or 3 points |
| `scoreHome` / `scoreAway` | Score at time of play |

### dbt layers

**`stg_plays`** — cleaned raw plays, renamed columns (e.g. `xLegacy` → `x`). All event types.

**`int_shots`** — field goal attempts only. Has everything: game context, clock parsed to seconds, home/away indicator. **This is where you add features.**

**`gold_shots`** — what the training DAG reads. Intentionally minimal — `game_id`, `action_number`, `x`, `y`, `shot_result`. You earn features by adding columns here from `int_shots`.

---

## Tips

- **Features flow automatically** — every column in `gold_shots` except `game_id`, `action_number`, and `shot_result` is used as a model feature. Add a column to gold, re-run dbt, re-run train.
- **`int_shots` has everything** — look there before writing new SQL. `clock_seconds`, `is_home_shooter`, shot type, game context are all already computed.
- **Non-numeric columns fill to 0** — encode categoricals (e.g. shot type) as integers in dbt before promoting to gold.
- **`shot_result` is NULL for eval rows** — these are the shots you're predicting. Never filter them out of gold.
- **You must submit all 250 eval rows** to appear on the leaderboard.

---

## Why no ingestion DAG?

The data is fixed for this exercise — everyone needs the same starting point. In production you'd have an ingestion pipeline; we built one earlier this semester. Here we ship `nba.duckdb` so you can focus on features and models.

---

## Submission

You get credit for leaderboard accuracy **and** for using dbt and Airflow correctly. Getting a good score by circumventing the pipeline won't earn full credit.
