/*
    dim_users
    =========
    User dimension table implementing an SCD Type 2 approach with:
    - Latest user profile attributes
    - Lifetime engagement metrics (total events, total sessions, active days)
    - First / last seen dates
    - User segment classification (power_user, regular, casual, dormant)
    - Valid_from / valid_to for slowly changing dimension tracking

    Grain: one row per user (current record).
*/

with users as (
    select * from {{ ref('stg_users') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

sessions as (
    select * from {{ ref('int_user_sessions') }}
),

-- Lifetime event metrics per user
user_event_metrics as (
    select
        user_id,
        count(*) as lifetime_event_count,
        count(distinct event_type) as distinct_event_types,
        count(distinct cast(event_at as date)) as active_days,
        min(event_at) as first_event_at,
        max(event_at) as last_event_at
    from events
    group by user_id
),

-- Lifetime session metrics per user
user_session_metrics as (
    select
        user_id,
        count(*) as lifetime_session_count,
        avg(session_duration_seconds) as avg_session_duration_seconds,
        avg(session_event_count) as avg_events_per_session,
        max(session_started_at) as last_session_at
    from sessions
    group by user_id
),

-- Join everything together
enriched as (
    select
        u.user_id,
        u.email,
        u.first_name,
        u.last_name,
        u.full_name,
        u.signed_up_at,
        u.plan_type,
        u.country,
        u.language,
        u.is_active,
        u.is_verified,

        -- Event metrics
        coalesce(em.lifetime_event_count, 0) as lifetime_event_count,
        coalesce(em.distinct_event_types, 0) as distinct_event_types,
        coalesce(em.active_days, 0) as active_days,
        em.first_event_at as first_seen_at,
        em.last_event_at as last_seen_at,

        -- Session metrics
        coalesce(sm.lifetime_session_count, 0) as lifetime_session_count,
        coalesce(sm.avg_session_duration_seconds, 0) as avg_session_duration_seconds,
        coalesce(sm.avg_events_per_session, 0) as avg_events_per_session,

        -- Derived: days since last activity
        datediff('day', em.last_event_at, current_timestamp()) as days_since_last_seen,

        -- Derived: tenure in days
        datediff('day', u.signed_up_at, current_date()) as tenure_days

    from users u
    left join user_event_metrics em on u.user_id = em.user_id
    left join user_session_metrics sm on u.user_id = sm.user_id
),

-- User segmentation
segmented as (
    select
        *,
        case
            when lifetime_event_count >= 500 and active_days >= 30
                then 'power_user'
            when lifetime_event_count >= 50 and active_days >= 7
                then 'regular'
            when lifetime_event_count >= 1
                then 'casual'
            else 'dormant'
        end as user_segment,

        -- SCD Type 2 fields
        u.updated_at as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current

    from enriched
    -- Bring in updated_at for SCD tracking
    inner join {{ ref('stg_users') }} u on enriched.user_id = u.user_id
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'valid_from']) }} as user_key,
    *
from segmented
