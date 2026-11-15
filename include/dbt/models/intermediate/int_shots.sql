-- Field goal attempts only. This is your feature engineering layer.
-- Add derived columns here, then promote them to gold_shots.

with shots as (
    select * from {{ ref('stg_plays') }}
    where is_field_goal = 1
),

games as (
    select * from {{ ref('stg_games') }}
),

joined as (
    select
        -- Identifiers
        s.game_id,
        s.action_number,

        -- Raw coordinates
        s.x,
        s.y,

        -- Target
        s.shot_result,

        -- Example: parse the clock string (PT11M32.00S) to seconds remaining
        cast(
            regexp_extract(s.clock, 'PT(\d+)M', 1) as integer
        ) * 60
        + cast(
            regexp_extract(s.clock, 'M(\d+)', 1) as integer
        ) as clock_seconds,

        -- Example: home or away shooter (not a strong feature, but shows the pattern)
        case when s.location = 'h' then 1 else 0 end as is_home_shooter

        -- ----------------------------------------------------------------
        -- Add your own features below.
        -- Hint: stg_plays and stg_games have more columns than what's shown
        -- here. Think about what you know at the moment a shot is taken.
        -- ----------------------------------------------------------------

    from shots s
    left join games g on s.game_id = g.game_id
)

select * from joined
