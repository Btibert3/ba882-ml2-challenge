-- Gold layer: one row per shot attempt.
-- shot_result is NULL for the leaderboard eval set — do not filter those out.
-- The training DAG handles the split: train on known results, score the rest.

select
    game_id,
    action_number,
    period,
    clock_seconds,
    shot_distance,
    shot_value,
    is_home_shooter,
    sub_type,
    player_name,
    team_tricode,
    x,
    y,
    shot_result
from {{ ref('int_shots') }}
