-- Gold layer: one row per shot attempt.
-- shot_result is NULL for the leaderboard eval set — do not filter those out.
-- The training DAG handles the split: train on known results, score the rest.

select
    game_id,
    action_number,
    x,
    y,
    shot_result
from {{ ref('int_shots') }}
