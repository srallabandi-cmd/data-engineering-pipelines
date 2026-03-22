/*
    int_user_sessions
    =================
    Sessionise user events using a 30-minute inactivity gap.

    Logic:
    1. Order events per user by timestamp.
    2. Compute time gap to previous event.
    3. Mark a new session when the gap exceeds 30 minutes (or is the first event).
    4. Assign a cumulative session index per user.
    5. Build a deterministic session_id from user_id + session index.
    6. Aggregate session-level metrics (duration, event count, entry/exit pages).
*/

with events as (
    select
        event_id,
        user_id,
        event_type,
        event_at,
        page_name,
        device_type,
        duration_seconds
    from {{ ref('stg_events') }}
),

-- Step 1-2: compute gap to previous event
with_gap as (
    select
        *,
        lag(event_at) over (
            partition by user_id order by event_at
        ) as prev_event_at,
        datediff(
            'second',
            lag(event_at) over (partition by user_id order by event_at),
            event_at
        ) as seconds_since_prev
    from events
),

-- Step 3: flag new sessions (gap > 30 min or first event)
with_session_flag as (
    select
        *,
        case
            when prev_event_at is null then 1
            when seconds_since_prev > 1800 then 1
            else 0
        end as is_new_session
    from with_gap
),

-- Step 4: cumulative session index
with_session_index as (
    select
        *,
        sum(is_new_session) over (
            partition by user_id
            order by event_at
            rows between unbounded preceding and current row
        ) as session_index
    from with_session_flag
),

-- Step 5: deterministic session_id
with_session_id as (
    select
        *,
        user_id || '_' || cast(session_index as varchar) as session_id
    from with_session_index
),

-- Step 6: session-level aggregates
session_summary as (
    select
        session_id,
        user_id,
        min(event_at) as session_started_at,
        max(event_at) as session_ended_at,
        datediff('second', min(event_at), max(event_at)) as session_duration_seconds,
        count(*) as session_event_count,
        count(distinct event_type) as distinct_event_types,
        first_value(page_name) over (
            partition by session_id order by event_at
            rows between unbounded preceding and unbounded following
        ) as entry_page,
        last_value(page_name) over (
            partition by session_id order by event_at
            rows between unbounded preceding and unbounded following
        ) as exit_page,
        first_value(device_type) over (
            partition by session_id order by event_at
            rows between unbounded preceding and unbounded following
        ) as device_type
    from with_session_id
    group by
        session_id,
        user_id,
        page_name,
        device_type,
        event_at
)

select distinct
    session_id,
    user_id,
    session_started_at,
    session_ended_at,
    session_duration_seconds,
    session_event_count,
    distinct_event_types,
    entry_page,
    exit_page,
    device_type,
    cast(session_started_at as date) as session_date
from session_summary
