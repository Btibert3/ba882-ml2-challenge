with source as (
    select * from {{ source('raw', 'raw_plays') }}
)

select
    gameId       as game_id,
    actionNumber as action_number,
    actionId     as action_id,
    period,
    clock,
    teamId       as team_id,
    teamTricode  as team_tricode,
    personId     as person_id,
    playerName   as player_name,
    xLegacy      as x,
    yLegacy      as y,
    shotDistance as shot_distance,
    shotResult   as shot_result,
    isFieldGoal  as is_field_goal,
    scoreHome    as score_home,
    scoreAway    as score_away,
    location,
    subType      as sub_type,
    shotValue    as shot_value
from source
