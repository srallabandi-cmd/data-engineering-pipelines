{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'sessions']
    )
}}

/*
    Intermediate model: User Sessions

    Sessionizes raw events using a 30-minute inactivity timeout.
    Computes per-session metrics:
    - Session start and end timestamps
    - Session duration
    - Event count and distinct event types
    - First and last event details
    - Conversion flag

    Sessionization logic:
    1. Order events by user_id, event_timestamp
    2. Compute time gap to previous event (per user)
    3. Flag new session when gap > 30 minutes
    4. Assign session_id via running sum of session flags
*/

with events as (

    select * from {{ ref('stg_events') }}
    where user_id != 'anonymous'

),

-- Step 1: Compute time gap to previous event per user
with_lag as (

    select
        *,
        lag(event_timestamp) over (
            partition by user_id
            order by event_timestamp
        ) as prev_event_timestamp,

        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id
                order by event_timestamp
            )
        )) as gap_seconds

    from events

),

-- Step 2: Flag new sessions (gap > 30 min or first event)
with_session_flag as (

    select
        *,
        case
            when prev_event_timestamp is null then 1
            when gap_seconds > ({{ var('session_timeout_minutes') }} * 60) then 1
            else 0
        end as is_new_session

    from with_lag

),

-- Step 3: Assign session index via running sum
with_session_index as (

    select
        *,
        sum(is_new_session) over (
            partition by user_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as session_index

    from with_session_flag

),

-- Step 4: Build session_id
with_session_id as (

    select
        *,
        user_id || '_' || cast(session_index as varchar) as session_id

    from with_session_index

),

-- Step 5: Aggregate per session
session_aggregates as (

    select
        session_id,
        user_id,

        -- Timestamps
        min(event_timestamp)    as session_start_at,
        max(event_timestamp)    as session_end_at,

        -- Duration
        extract(epoch from (
            max(event_timestamp) - min(event_timestamp)
        ))                      as session_duration_seconds,

        -- Counts
        count(*)                as session_event_count,
        count(distinct event_type)  as session_distinct_event_types,

        -- First and last event
        first_value(event_type) over (
            partition by session_id
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        )                       as first_event_type,

        last_value(event_type) over (
            partition by session_id
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        )                       as last_event_type,

        -- Engagement
        sum(duration_ms)        as total_duration_ms,
        sum(revenue)            as session_revenue,
        max(case when is_conversion then 1 else 0 end) as has_conversion,

        -- Context
        min(device_type)        as device_type,
        min(country)            as country,
        min(event_date)         as session_date

    from with_session_id
    group by
        session_id,
        user_id,
        -- window function columns require grouping context
        event_type,
        event_timestamp

),

-- Deduplicate the window function results per session
final_sessions as (

    select distinct
        session_id,
        user_id,
        session_start_at,
        session_end_at,
        session_duration_seconds,
        session_event_count,
        session_distinct_event_types,
        first_event_type,
        last_event_type,
        total_duration_ms,
        session_revenue,
        has_conversion,
        device_type,
        country,
        session_date,

        -- Session classification
        case
            when session_event_count = 1 then 'bounce'
            when session_duration_seconds < 30 then 'short'
            when session_duration_seconds < 300 then 'medium'
            when session_duration_seconds < 1800 then 'long'
            else 'very_long'
        end as session_category

    from session_aggregates

)

select * from final_sessions
