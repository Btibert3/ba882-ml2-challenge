"""
Pulls NBA play-by-play data into DuckDB.

Run this once at the start of class to load your raw tables.
When it finishes, it automatically triggers the dbt DAG to build your models.

You should not need to edit this DAG.

Flow:
  fetch_teams ──┐
  fetch_games ──┼── load into DuckDB (raw_teams, raw_games, raw_plays)
  fetch_plays   │   (.expand() fans out one task per game)
  (per game_id) ┘
       └── triggers dbt_dag
"""

import os
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

API_BASE = os.environ.get("API_BASE", "https://btibert-bu--ba882-nba-ship-it")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str((Path(__file__).parent / "../nba.duckdb").resolve()))


def get_conn():
    return duckdb.connect(DUCKDB_PATH)


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nba", "ingest"],
)
def ingest():

    @task()
    def fetch_teams():
        resp = requests.get(f"{API_BASE}-teams.us-east.modal.run", timeout=30)
        resp.raise_for_status()
        teams = resp.json()

        con = get_conn()
        con.execute("DROP TABLE IF EXISTS raw_teams")
        con.execute("""
            CREATE TABLE raw_teams (
                team_id      BIGINT,
                full_name    VARCHAR,
                abbreviation VARCHAR,
                city         VARCHAR
            )
        """)
        con.executemany(
            "INSERT INTO raw_teams VALUES (?, ?, ?, ?)",
            [(r["team_id"], r["full_name"], r["abbreviation"], r["city"]) for r in teams],
        )
        con.close()
        print(f"loaded {len(teams)} teams")

    @task()
    def fetch_games():
        resp = requests.get(f"{API_BASE}-games.us-east.modal.run", timeout=30)
        resp.raise_for_status()
        games = resp.json()

        con = get_conn()
        con.execute("DROP TABLE IF EXISTS raw_games")
        con.execute("""
            CREATE TABLE raw_games (
                game_id      VARCHAR,
                game_date    VARCHAR,
                home_team_id BIGINT,
                home_team    VARCHAR,
                away_team_id BIGINT,
                away_team    VARCHAR
            )
        """)
        con.executemany(
            "INSERT INTO raw_games VALUES (?, ?, ?, ?, ?, ?)",
            [(r["game_id"], r["game_date"], r["home_team_id"], r["home_team"],
              r["away_team_id"], r["away_team"]) for r in games],
        )
        con.close()
        print(f"loaded {len(games)} games")
        return [r["game_id"] for r in games]

    @task()
    def setup_plays_table():
        con = get_conn()
        con.execute("DROP TABLE IF EXISTS raw_plays")
        con.execute("""
            CREATE TABLE raw_plays (
                gameId       VARCHAR,
                actionNumber INTEGER,
                actionId     INTEGER,
                clock        VARCHAR,
                period       INTEGER,
                teamId       BIGINT,
                teamTricode  VARCHAR,
                personId     BIGINT,
                playerName   VARCHAR,
                xLegacy      INTEGER,
                yLegacy      INTEGER,
                shotDistance INTEGER,
                shotResult   VARCHAR,
                isFieldGoal  INTEGER,
                scoreHome    VARCHAR,
                scoreAway    VARCHAR,
                location     VARCHAR,
                description  VARCHAR,
                actionType   VARCHAR,
                subType      VARCHAR,
                shotValue    INTEGER,
                PRIMARY KEY (gameId, actionNumber)
            )
        """)
        con.close()

    @task()
    def fetch_plays(game_id: str):
        resp = requests.get(
            f"{API_BASE}-plays.us-east.modal.run",
            params={"game_id": game_id},
            timeout=60,
        )
        resp.raise_for_status()
        plays = resp.json()

        con = get_conn()
        con.executemany("""
            INSERT OR IGNORE INTO raw_plays
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                str(p.get("gameId", "")).zfill(10),
                p.get("actionNumber"),
                p.get("actionId"),
                p.get("clock"),
                p.get("period"),
                p.get("teamId"),
                p.get("teamTricode"),
                p.get("personId"),
                p.get("playerName"),
                p.get("xLegacy"),
                p.get("yLegacy"),
                p.get("shotDistance"),
                p.get("shotResult"),
                p.get("isFieldGoal"),
                p.get("scoreHome"),
                p.get("scoreAway"),
                p.get("location"),
                p.get("description"),
                p.get("actionType"),
                p.get("subType"),
                p.get("shotValue"),
            )
            for p in plays
        ])
        con.close()
        shots = sum(1 for p in plays if p.get("isFieldGoal") == 1)
        print(f"game {game_id}: {len(plays)} events, {shots} shots")

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_dag",
        wait_for_completion=False,
    )

    teams = fetch_teams()
    game_ids = fetch_games()
    setup = setup_plays_table()
    plays = fetch_plays.expand(game_id=game_ids)

    [teams, setup] >> plays >> trigger_dbt


ingest()
