with source as (
    select * from {{ source('raw', 'raw_teams') }}
)

select
    team_id,
    full_name,
    abbreviation,
    city
from source
