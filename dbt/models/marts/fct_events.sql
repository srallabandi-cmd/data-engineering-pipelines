/*
    fct_events
    ==========
    Events fact table at the event grain, enriched with:
    - User dimension attributes (plan_type, country, user_segment)
    - Session context (session_id, session_duration, entry/exit pages)
    - Deduplication via row_number on event_id

    Grain: one row per unique event.
*/

with events as (
    select * from {{ ref('stg_events') }}
),

users as (
    select
        user_id,
        plan_type,
        country,
        user_segment,
        signed_up_at,
        is_active
    from {{ ref('dim_users') }}
    where is_current = true
),

sessions as (
    select * from {{ ref('int_user_sessions') }}
),

-- Deduplicate events (keep the earliest loaded record per event_id)
deduplicated as (
    select
        *,
        row_number() over (
            partition by event_id
            order by loaded_at asc
        ) as _row_num
    from events
),

unique_events as (
    select * from deduplicated where _row_num = 1
),

-- Map events to sessions by checking if the event timestamp falls within
-- a session's start/end boundaries for the same user.
events_with_sessions as (
    select
        e.*,
        s.session_id,
        s.session_duration_seconds,
        s.session_event_count,
        s.entry_page as session_entry_page,
        s.exit_page as session_exit_page,
        s.device_type as session_device_type
    from unique_events e
    left join sessions s
        on e.user_id = s.user_id
        and e.event_at >= s.session_started_at
        and e.event_at <= s.session_ended_at
),

-- Join with user dimension
enriched as (
    select
        e.event_id,
        e.user_id,
        e.event_type,
        e.event_at,
        cast(e.event_at as date) as event_date,
        extract(hour from e.event_at) as event_hour,
        extract(dow from e.event_at) as event_day_of_week,
        e.page_name,
        e.referrer,
        e.device_type,
        e.duration_seconds,
        e.platform,

        -- Session context
        e.session_id,
        e.session_duration_seconds,
        e.session_event_count,
        e.session_entry_page,
        e.session_exit_page,

        -- User dimension
        u.plan_type as user_plan_type,
        u.country as user_country,
        u.user_segment,
        u.signed_up_at as user_signed_up_at,
        u.is_active as user_is_active,

        -- Derived: is this user's first event?
        case
            when row_number() over (
                partition by e.user_id order by e.event_at asc
            ) = 1 then true
            else false
        end as is_first_event,

        e.loaded_at

    from events_with_sessions e
    left join users u on e.user_id = u.user_id
)

select * from enriched
