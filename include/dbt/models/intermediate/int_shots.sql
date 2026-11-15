-- Filter to field goal attempts only and join game context.
-- This is your feature engineering layer — add columns here.

with shots as (
    select * from {{ ref('stg_plays') }}
    where is_field_goal = 1
),

games as (
    select * from {{ ref('stg_games') }}
),

joined as (
    select
        s.game_id,
        s.action_number,
        s.period,
        s.clock,
        s.team_id,
        s.team_tricode,
        s.player_name,
        s.shot_distance,
        s.shot_value,
        s.sub_type,
        s.x,
        s.y,
        s.location,
        s.description,
        s.shot_result,

        -- Game context
        g.home_team,
        g.away_team,
        g.game_date,

        -- Is the shooter on the home team?
        case when s.location = 'h' then 1 else 0 end as is_home_shooter,

        -- Parse clock string (PT11M32.00S) to seconds remaining in period
        cast(
            regexp_extract(s.clock, 'PT(\d+)M', 1) as integer
        ) * 60
        + cast(
            regexp_extract(s.clock, 'M(\d+)', 1) as integer
        ) as clock_seconds

        -- ----------------------------------------------------------------
        -- TODO: add your own features below
        -- Ideas: shot angle from basket, distance buckets, is_three,
        --        clutch time (period=4 and clock_seconds < 120), etc.
        -- ----------------------------------------------------------------

    from shots s
    left join games g on s.game_id = g.game_id
)

select * from joined
