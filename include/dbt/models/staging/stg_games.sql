with source as (
    select * from {{ source('raw', 'raw_games') }}
)

select
    game_id,
    game_date,
    home_team_id,
    home_team,
    away_team_id,
    away_team
from source
